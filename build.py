from zipfile import ZipFile
import os
from datetime import datetime
import warnings
import time
import itertools

import gtfs_tripify as gt

# Change these paths to match the locations of these files and the desired output path on your machine.
OUTPUT_DIR = './data'
PATH_TO_GTFS_RT_ARCHIVE_FILE = os.path.expanduser('~/Downloads/201906.zip')
PATH_TO_GTFS_ARCHIVE_FILE = os.path.expanduser('~/Downloads/google_transit/stops.txt')


# Before you run the code that follows you will first need to download the data. To do so, visit 
# http://web.mta.info/developers/data/archives.html in your web browser and click on the "June 2019" link.
ZipFile(
    os.path.expanduser(f'{OUTPUT_DIR}/201906.zip')
).extract('20190601.zip', OUTPUT_DIR)
ZipFile('./data/20190601.zip').extractall(f'{OUTPUT_DIR}/20190601/')

def splitname(name):
    origname = name
    name = name[5:]
    first_splitter_idx = name.find('_')
    trainlines = name[:first_splitter_idx]
    
    name = name[first_splitter_idx + 1:]
    second_splitter_idx = name.find('_')
    date = name[:second_splitter_idx]
    
    name = name[second_splitter_idx + 1:][:-5]
    
    return origname, trainlines

# The unit of value in GTFS-RT data is a **message**. Each message is a record of expected arrival and
# departure times as of a specific timestamp. Messages are organized as a set of **feeds**: sequential
# time-series of messages covering a specific subset of trains in the MTA system.
#
# The archival data we just read in and extracted provides this information in the filenames. For
# example, the file `gtfs_7_20190601_042000.gtfs` tells us that it contains a snapshot of the state of
# all 7 trains in the MTA system as of 4:20 AM, June 1st, 2019.
#
# To extract arrival time estimates from these data streams we must parse them first by passing them
# through `gtfs_tripify`. In doing so, we must be very careful to keep the messages in sequential
# order, and to parse them one feed at a time.
# 
# The next block of code arranges pointers to the data into a variable `feed_message_map` which in a
# format convenient for passing to `gtfs_tripify`:
# 
# The list of messages in each feed is split into four segments. `gtfs_tripify` does all of its
# processing in-memory, and if we feed it too many messages at once it may overflow our computer's
# RAM. By processing the data in four chunks, six hours at a time, then combining those chunks at the
# end, we get same result using a quarter of the compute.
messageinfo = list(map(splitname, sorted(os.listdir(f'{OUTPUT_DIR}/20190601/'))))
unique_feeds = set([feed_identifier for (filename, feed_identifier) in messageinfo])
feed_message_map = dict()
for feed in unique_feeds:
    feed_message_map.update({
        feed: sorted([
            './data/20190601/' + filename for\
            (filename, feed_identifier) in messageinfo if feed_identifier == feed
        ])
    })
    
for feed_identifier in feed_message_map:
    feedlist = feed_message_map[feed_identifier]

    n_segments = 4
    n_messages = len(feedlist)
    step_size = len(feedlist) // n_segments
    offsets = list(range(0, n_messages, n_messages // n_segments))[1:]
    chunk_offsets = list(zip(list(range(0, n_messages, step_size)[:-1]), 
                             list(range(0, n_messages, step_size)[1:])))
    chunk_offsets[-1] = (chunk_offsets[-1][0], len(feedlist))    
    chunks = [feedlist[start:stop] for (start, stop) in chunk_offsets]
    feed_message_map[feed_identifier] = chunks
    
logbooks = dict()
timestamps = dict()
parse_errors = dict()

# It's now time to parse the data. Although the code in this section and the previous section
# can be used to parse data a full day's worth of data for every line, for the purposes of this
# demo analysis we will focus on a single specific line&mdash;the 7 train.
#
# The 7 train has its own dedicated feed, the `7` feed, so it's one of the fastest feeds to process,
# as it has the fewest trains total and hence, the smallest average message size. You can expand
# the code below to cover more train lines by adding more feed identifiers from the
# `feed_message_map.keys()` output above to the list below.
#
# All of the work in this code block is done by the gt.logify(messages) line (the rest is just
# housekeeping).
print(f'Started parsing chunks at {datetime.fromtimestamp(time.time()).strftime("%c")}')
for feed_identifier in ['7']:
    chunks = feed_message_map[feed_identifier]
    sublogs, sublogs_timestamps, sublogs_parse_errors = [], [], []

    for chunk in chunks:
        messages = []

        for filename in chunk:
            with open(filename, 'rb') as fp:
                messages.append(fp.read())
    
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            sublog, sublog_timestamps, sublog_parse_errors = gt.logify(messages)

        sublogs.append(sublog)
        sublogs_timestamps.append(sublog_timestamps)
        sublogs_parse_errors.append(sublog_parse_errors)
        print(f'Finishing parsing a chunk at {datetime.fromtimestamp(time.time()).strftime("%c")}')

    logbooks[feed_identifier], _ = gt.ops.merge_logbooks(list(zip(sublogs, sublogs_timestamps)))
    parse_errors[feed_identifier] = list(itertools.chain(*sublogs_parse_errors))

# We also need to run the `gt.ops.cut_cancellations` method. This will heuristically remove
# cancelled stops from the dataset
logbooks['7'] = gt.ops.cut_cancellations(logbooks['7'])

# Save to output format
stops = pd.read_csv(PATH_TO_GTFS_ARCHIVE_FILE).set_index('stop_id')

def get_stop_name_for_stop_id(stop_id):
    if stop_id in stops.index:
        return stops.loc[stop_id].stop_name
    else:  # the stop id is an unknown one
        return None

# There's one last thing we need to do: we're going to match the `stop_id` values onto the actual
# human-readable stop names. The key to this is the `stops.txt` record in the MTA's GTFS dataset,
# which you can get [here](http://web.mta.info/developers/developer-data-terms.html).
for unique_trip_id in logbooks['7']:
    df = logbooks['7'][unique_trip_id]
    stop_names = df.stop_id.map(get_stop_name_for_stop_id)
    df = df.assign(stop_name=stop_names)
    logbooks['7'][unique_trip_id] = df
    
gt.ops.to_csv(logbooks['7'], f'{OUTPUT_DIR}/7_trains.csv')
