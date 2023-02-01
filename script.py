import pandas as pd

# create dataframe
columns = ('index-name', 'shard-number', 'type', 'state', 'document-count', 'store-size', 'node-IP', 'node-name')
df = pd.read_csv('data', sep=' ', names=columns, header=None)

# 1. Number of primary shards and replica shards, separately.
type_count = df['type'].value_counts()
print(f"count: [primary: {type_count['p']}, replica: {type_count['r']}]")


# 2. convert all values to mb for simplified calculation
def convert(value):
    size, unit = float(value[:-2]), value[-2:]
    if unit == 'kb':
        return size / 1024
    elif unit == 'gb':
        return size * 1024
    elif unit == 'tb':
        return size * 1024 * 1024
    else:
        return size


df['store-size'].fillna('0mb', inplace=True)  # fill na values
df['size_in_mb'] = df['store-size'].map(lambda x: convert(x))
print(
    f"size: [primary: {round(df[df['type'] == 'p']['size_in_mb'].sum(), 1)}mb, "
    f"replica: {round(df[df['type'] == 'r']['size_in_mb'].sum(), 1)}mb]")

# 3. Name of the elasticsearch node where the disk usage is maximum out of all nodes.
print(f"disk-max-node: {df[df['size_in_mb'] == df['size_in_mb'].max()]['node-name'].values[0]}")


# 4. Assume a 128gb disk for each node; list the nodes where the 80% disk watermark has been crossed.
items = []


def find_threshold_crossed():
    max_val = 128 * 1024
    for i, x in df.iterrows():
        if (x['size_in_mb'] / max_val) * 100 > 80:
            items.append(x['node-name'])


find_threshold_crossed()
print(f'watermark-breached: {items}')
