import json
import shlex
from datetime import datetime, timedelta, timezone
import subprocess
from pathlib import Path
import argparse
import sys


current_time = datetime.now()
current_time_utc = datetime.now(timezone.utc)
print(current_time)

def get_slurm_log(config, start_time, end_time):

    print("getting data between ", start_time, ' and ', end_time, ' on ', config['cluster name'])
    
    sacct = config.get('sacct', None)
    if not sacct:
      return None
    # call sacct in UTC to avoid time zone issues
    env = {'TZ': 'UTC'}
    
    # some systems need the path to slurm config provided
    if "SLURM_CONF" in config:
      env['SLURM_CONF'] = config['SLURM_CONF']
    
    flags = "-P --allusers --allocations --duplicates"
    outputs = "Jobid,Jobidraw,account,user,Submit,Start,End,alloctres,QOS"
    states = "BF,CA,CD,DL,F,NF,OOM,PR,RQ,RS,RV,S,TO"
    cmd = "{} --clusters {} {} -o {} -s {} --starttime {} --endtime {}".format(
        sacct,
        config['cluster name'],
        flags,
        outputs,
        states,
        start_time,
        end_time
    )
    result = subprocess.run(shlex.split(cmd), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    if result.returncode == 0:
        return result.stdout.decode().strip()
    else:
        return None

if __name__ == "__main__":

    # Set up command line options
    parser = argparse.ArgumentParser(description='Get usage by user for specific slurm account(s)')
    parser.add_argument('-c', '--config', help="Cluster config file", required=True)

    # parse the input
    args = parser.parse_args()
    
    try:
        with open(args.config, 'r', encoding='utf-8') as file:
            orig_config = json.load(file)
        config = orig_config.copy()
    except Exception as e:
        sys.exit("failed to read config file ({}): {}.".format(args.metrics, e))

    last_end_time = datetime.fromisoformat(orig_config['end_time'])
    last_start_time = datetime.fromisoformat(orig_config['start_time'])
    start_time = last_end_time + timedelta(0, 1)
    end_time = current_time_utc + timedelta(0, -60)
    
    logs = get_slurm_log(config, start_time, end_time)
    if logs:
        try:
            outpath = Path(config['output directory']).joinpath(config['cluster name'] + '_' + end_time.strftime('%Y-%m-%dT%T') + '.log')
            print("saving to:", outpath)
            with open(outpath, "w") as file:
                file.write(logs)
            config['start_time'] = start_time.strftime('%Y-%m-%dT%T')
            config['end_time'] = end_time.strftime('%Y-%m-%dT%T')
    
            # save new config so the next run has the correct times
            with open(args.config, 'w') as f:
                json.dump(config, f)
        except Exception as e:
            sys.exit("failed to save logs to {}: {}.".format(outpath, e))
