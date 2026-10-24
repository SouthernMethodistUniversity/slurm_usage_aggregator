import argparse
import datetime
from pathlib import Path
import pandas as pd
import sys
import json

def get_all_accounts(df):

    return df['Account'].unique().tolist()

def ingest_file(file_path, config):

    if file_path:

        df = pd.read_csv(file_path, sep="|")
        date_cols = ['End', 'Start', 'Submit']
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='ISO8601', utc=True)

        df = df.dropna(subset = date_cols).reset_index(drop=True)
        df = df.assign(**{"Cluster": config['cluster_name']})
       
        # split jobs spanning multiple days into separate rows
        date_edges = pd.date_range(df['Submit'].min().normalize() - pd.Timedelta(days=1),
                      df['End'].max().normalize() + pd.Timedelta(days=1), freq='D')
        
        # waiting on multiple days
        new_rows = []
        wait_df = df[['JobID', 'Submit', 'Start', 'User', 'Account', 'Cluster', 'QOS']]
        for edge in date_edges:
            mask = (wait_df['Submit'] < edge) & (wait_df['Start'] > edge)
            tmp_df = wait_df.loc[mask]
            if not tmp_df.empty:
                tmp_df = tmp_df.assign(**{"Start": edge})
                new_rows.append(tmp_df)
                wait_df.loc[mask, 'Submit'] = edge
        if new_rows:
            wait_df = pd.concat(new_rows+[wait_df]).sort_values('Submit').reset_index(drop=True)

        # running on multiple days
        new_rows = []
        usage_df = df[['JobID', 'Start', 'End', 'User', 'Account', 'Cluster', 'QOS', 'AllocTRES']]
        for edge in date_edges:
            mask = (usage_df['Start'] < edge) & (usage_df['End'] > edge)
            tmp_df = usage_df.loc[mask]
            if not tmp_df.empty:
                tmp_df = tmp_df.assign(**{"End": edge})
                new_rows.append(tmp_df)
                usage_df.loc[mask, 'Start'] = edge
        if new_rows:
            usage_df = pd.concat(new_rows+[usage_df]).sort_values('Start').reset_index(drop=True)

        # compute wait times and run times
        wait_df['Wait Time'] = (wait_df['Start'] - wait_df['Submit']).dt.total_seconds() / 3600.0
        usage_df['Run Time'] = (usage_df['End'] - usage_df['Start']).dt.total_seconds() / 3600.0

        # split out TRES info
        for key, vals in config["AllocTRES"].items():
            usage_df[key] = usage_df['AllocTRES'].str.extract(vals['pattern']).fillna(str(vals['default'])).astype(vals['type'])
            normalize_mem = vals.get("normalize memory units", False)
            if normalize_mem:
                usage_df['mem unit'] = usage_df['AllocTRES'].str.extract('mem=\\d*(\\D),')
                usage_df[usage_df['mem unit'] == 'T'][key] = usage_df[usage_df['mem unit'] == 'T'][key] * 1024
                usage_df[usage_df['mem unit'] == 'M'][key] = usage_df[usage_df['mem unit'] == 'M'][key] / 1024
                usage_df[usage_df['mem unit'] == 'K'][key] = usage_df[usage_df['mem unit'] == 'K'][key] / (1024 * 1024)
                usage_df = usage_df.drop(columns='mem unit')

            usage_df[key] = usage_df['Run Time'] * usage_df[key]

        wait_aggregate_data = []
        for edge in date_edges:
            start_time = edge - pd.to_timedelta(1, unit='D')
            mask = ((wait_df['Submit'] >= start_time) & (wait_df['Submit'] < edge))
            tmp_df = wait_df.loc[mask]
            
            agg_df = tmp_df[['Account', 'User', 'Cluster', 'QOS', 'Wait Time']].groupby(['Account', 'User', 'Cluster', 'QOS']).aggregate('sum')

            if not agg_df.empty:
                agg_df['Date'] = start_time
                wait_aggregate_data.append(agg_df)

        usage_aggregate_data = []
        for edge in date_edges:
            start_time = edge - pd.to_timedelta(1, unit='D')
            mask = ((usage_df['Start'] >= start_time) & (usage_df['Start'] < edge))
            tmp_df = usage_df.loc[mask]
            agg_df = tmp_df[['Account', 'User', 'Cluster', 'QOS'] + list(config["AllocTRES"])].groupby(['Account', 'User', 'Cluster', 'QOS']).aggregate('sum')

            if not agg_df.empty:
                agg_df['Date'] = start_time
                usage_aggregate_data.append(agg_df)

        wait_df =  pd.concat(wait_aggregate_data).sort_values('Date').reset_index()
        usage_df =  pd.concat(usage_aggregate_data).sort_values('Date').reset_index()
        df = pd.merge(usage_df, wait_df, on=['User', 'Account', 'Date', 'Cluster', 'QOS'], how='outer').fillna(0).reset_index(drop=True)

        return df

    else:
        return pd.DataFrame()
    
def merge_ingested_data(df, new_data, config):

    if new_data.empty:
        return df
    else:
        if df.empty:
            return new_data
        else:
            cols = ['Wait Time'] + list(config["AllocTRES"])
            for col in cols:
                new_data = new_data.rename(columns={col: 'New {}'.format(col)})

            df = pd.merge(df, new_data, on=['User', 'Account', 'Date', 'Cluster', 'QOS'], how='outer').fillna(0).reset_index(drop=True)
            
            for col in cols:
                df[col] = df[col] + df['New {}'.format(col)]
                df = df.drop(columns=['New {}'.format(col)])
            return df.reset_index(drop=True)
        
def get_formatting_order(df, config):
    
    possible_cols = ['Cluster', 'Account', 'User', 'QOS'] + list(["AllocTRES"]) + ['Wait Time']
    avail_cols = df.columns.tolist()
    aggregate_cols = []
    if config['aggregate_users']:
        aggregate_cols.append('User')
        
    if config['aggregate_qos']:
        aggregate_cols.append('QOS')
    
    if config['aggregate_cluster']:
        aggregate_cols.append('Cluster')
        
    allowed_cols = (set(possible_cols) & set(avail_cols)) - set(aggregate_cols)
    print_cols = [x for x in possible_cols if x in allowed_cols]
    config['print columns'] = print_cols
    config['aggregate columns'] = aggregate_cols
    
def print_usage(usage, config):

    if config['aggregate_cluster']:
        max_lengths = [7, 4, 9, 9, 12, 9]
    else:
        max_lengths = [7, 7, 4, 9, 9, 12, 9]
    for line in usage:
        for i in range(0,len(line)):
            max_lengths[i] = max(int(max_lengths[i]), int(len(str(line[i]))))
            
    # figure out spacing
    max_lengths = []
    for col in config['print columns']:
        max_lengths.append(len(col))

    total_len = 0
    for i in range(0, len(max_lengths)):
        max_lengths[i] += 3
        total_len += max_lengths[i]

    tmp_str = "{}: <{}}".format(config['print columns'][0], max_lengths[0])
    for i in range(1, len(config['print columns'])):
        tmp_str += "{: >{}}".format(config['print columns'][i], max_lengths[i])
    print(tmp_str)
        
    print('=' * total_len)
    for i in range(0,len(usage)):
        line = usage[i]
        tmp_str = "{}: <{}}".format(line[0], max_lengths[0])
        for i in range(1, len(config['print columns'])):
            tmp_str += "{: >{}}".format(line[i], max_lengths[i])
        print(tmp_str)

        
def parse_usage(df, config):

    accounts = list(set(config['accounts']) - set(config['exclude']))

    usage = []

    # account, user, cpu hours, gpu hours, mem hours, wait hours, etc
    mask = df['Account'].isin(config['accounts'])
    if config['users']:
        mask = mask & (df['User'].isin(config['users']))
    if config['start']:
        mask = mask & (df['Date'] >= config['start'])
    if config['end']:
        mask = mask & (df['Date'] <= config['end'])
    if config['clusters']:
        mask = mask & (df['Cluster'].isin(config['clusters']))
    if config['qos']:
        mask = mask & (df['QOS'].isin(config['qos']))

    tmp_df = df[mask]
    group_by_cols = ['User', 'Cluster', 'Account', 'QOS']
    group_by_cols = list(set(group_by_cols) - set(config['aggregate columns']))
    data_cols = list(set(group_by_cols + config['print columns']))
    cols_to_round = ['Wait Time'] + list(config["AllocTRES"])
    agg_dict = {}
    for col in data_cols:
        if col in cols_to_round:
            # numerical columns get summed
            agg_dict[col] = "sum"
        elif col in config['print columns']:
            # take first value of text col
            agg_dict[col] = "first"
    agg_df = tmp_df[data_cols].groupby(group_by_cols).aggregate(agg_dict).reset_index().sort_values(config['print columns'][0])
    for col in cols_to_round:
        agg_df[col] = agg_df[col].round(3)
    for index, row in agg_df.iterrows():
        tmp_entry = []
        for col in config['print columns']:
            tmp_entry.append(row[col])
        usage.append(tmp_entry)

    return usage

if __name__ == "__main__":

    # Set up command line options
    parser = argparse.ArgumentParser(description='Get usage by user for specific slurm account(s)')
    parser.add_argument('-a', '--accounts', help="SLURM account names to check, comma separated list")
    parser.add_argument('-A', '--All', help="Return usage for all SLURM accounts", default=False, action='store_true')
    parser.add_argument('-x', '--exclude', help="Comma separated list of accounts it ignore")
    parser.add_argument('-d', '--data', help="Path to data file. If the file exists, any imported data will be appended")
    parser.add_argument('-i', '--ingest', help="Path to of datafile to import.")
    parser.add_argument('--ingest_cluster', help="Name of cluster being ingested. Uses 'default' if none specified")
    parser.add_argument('-c', '--cluster', help="Comma separated list of clusters to include. Defaults to all clusters if none specified.")
    parser.add_argument('-u', '--users', help="Comma separated list of users to include. Defaults to all users if none specified.")
    parser.add_argument('-q', '--qos', help="Comma separated list of qos names to include. Defaults to all qos names if none specified.")
    parser.add_argument('-S', '--start', help="Start date (ISOformat, e.g. YYYY-MM-DD) to filter returns. Filter is inclusive (includes the specified date.) Does not filter if not specified.",
                        type=datetime.date.fromisoformat)
    parser.add_argument('-e', '--end', help="End date (ISOformat, e.g. YYYY-MM-DD) to filter returns. Filter is inclusive (includes the specified date.) Does not filter if not specified.",
                        type=datetime.date.fromisoformat)
    parser.add_argument('--silent', help="Do not print account information", default=False, action='store_true')
    parser.add_argument('--aggregate_cluster', help="Aggregate output cluster data (combine date for all clusters)", default=False, action='store_true')
    parser.add_argument('--aggregate_qos', help="Aggregate output qos data (combine data for all qos names)", default=False, action='store_true')
    parser.add_argument('--aggregate_users', help="Aggregate output user data (combine data for all users)", default=False, action='store_true')
    parser.add_argument('-m', '--metrics', help="Path to metrics config file. Defaults to looking in $HOME/.slurm_usage_aggregator/metrics.json and falls back to basic settings.")

    # parse the input
    args = parser.parse_args()

    config = {}

    if args.accounts:
        accounts = str(args.accounts).split(',')
        config['accounts'] = accounts
    else:
        config['accounts'] = []
        
    # default metrics values
    config["AllocTRES"] = {
        "CPU Hours": {
            "pattern": r"cpu=(\\d*),",
            "default": 0,
            "type": "int32"
        },
        "Memory Hours": {
            "pattern": "mem=(\\d*),",
            "default": 0,
            "type": "float64",
            "normalize memory units": True
        },
        "GPU Hours": {
            "pattern": "gres\\/gpu=(\\d*),",
            "default": 0,
            "type": "int32"
        }
    }
        
    if args.metrics:
        try:
            with open(args.metrics, 'r', encoding='utf-8') as file:
                data = json.load(file)
            config['AllocTRES'] = data['AllocTRES']
        except Exception as e:
            print("failed to read metrics config file ({}): {}. \nTrying default location".format(args.metrics, e))
            file_path = Path.home() / ".slurm_usage_aggregator" / "metrics.json"
            if file_path.is_file():
                try:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        data = json.load(file)
                    config['AllocTRES'] = data['AllocTRES']
                except Exception as e2:
                    print("failed to read metrics config file ({}): {}. \nUsing defaults".format(file_path, e))

    if args.All:
        config['all_accounts'] = True
    else:
        config['all_accounts'] = False

    if args.exclude:
        accounts = str(args.exclude).split(',')
        config['exclude'] = accounts
    else:
        config['exclude'] = []

    if args.users:
        users = str(args.users).split(',')
        config['users'] = users
    else:
        config['users'] = []
        
    if args.qos:
            qos = str(args.qos).split(',')
            config['qos'] = qos
    else:
            config['qos'] = []

    if args.cluster:
        clusters = str(args.cluster).split(',')
        config['clusters'] = clusters
    else:
        config['clusters'] = []

    if args.ingest_cluster:
        config['cluster_name'] = str(args.ingest_cluster).strip()
    else:
        config['cluster_name'] = 'default'

    if args.data:
        output = Path(args.data)
        config['data_path'] = str(output)
    else:
        config['data_path'] = ''

    if args.ingest:
        input = Path(args.ingest)
        config['ingest_file'] = str(input)
    else:
        config['ingest_file'] = ''

    if args.silent:
        config['silent'] = True
    else:
        config['silent'] = False

    if args.aggregate_cluster:
        config['aggregate_cluster'] = True
    else:
        config['aggregate_cluster'] = False
        
    if args.aggregate_qos:
        config['aggregate_qos'] = True
    else:
        config['aggregate_qos'] = False
        
    if args.aggregate_users:
        config['aggregate_users'] = True
    else:
        config['aggregate_users'] = False
        
    if config['aggregate_users'] and config['aggregate_qos'] and config['aggregate_cluster']:
        sys.exit('Cannot produce output with the provided aggregate flags. Please remove at least one.')

    if args.start:
        config['start'] = pd.Timestamp(args.start, tz="UTC").replace(microsecond=0)
    else:
        config['start'] = None

    if args.end:
        config['end'] = pd.Timestamp(args.end, tz="UTC").replace(microsecond=0)
    else:
        config['end'] = None

    if config['accounts'] == [] and not config['all_accounts']:
        sys.exit('No SLURM account(s) specified. Use --accounts=account1,account2,etc or -A for all accounts')

    if not config['ingest_file'] and not config['data_path']:
        sys.exit('A data file and/or a slurm log to ingest is required. Use -d path/to/file to specify and existing file and or -i /path/to/file.log to ingest new slurm data')

    if config['ingest_file'] == [] and not config['all_accounts']:
        sys.exit('No SLURM account(s) specified. Use --accounts=account1,account2,etc or -A for all accounts')

    ingested_df = ingest_file(config['ingest_file'], config)

    if config['data_path']:

        if Path(config['data_path']).exists():
            try:
                df = pd.read_feather(config['data_path'])
            except Exception as e:
                print("failed to read data file: {}".format(e))
                sys.exit()
        else:
            df = pd.DataFrame()

        df = merge_ingested_data(df, ingested_df)

        df.to_feather(config['data_path'])

    else:
        df = ingested_df

    if config['all_accounts']:
        config['accounts'] = get_all_accounts(df)

    get_formatting_order(df, config)
    usage = parse_usage(df, config)

    if not config['silent']:
        print_usage(usage, config)
