import hashlib
import os


def exec_hive_stat2(query, filename=None, priority=False, verbose=True, nice=False):
    """Query Hive."""
    if priority:
        query = "SET mapreduce.job.queuename=priority;" + query
    elif nice:
        query = "SET mapreduce.job.queuename=nice;" + query
        # if issues: SET mapred.job.queue.name=nice;
    cmd = """hive -e \" """ + query + """ \""""
    if filename:
        cmd = cmd + " > " + filename
    if verbose:
        print(cmd)
    ret = os.system(cmd)
    return ret


def exec_mariadb_stat2(query, db, filename=None, verbose=True):
    """Query MariaDB."""
    cmd = 'mysql --host analytics-slave.eqiad.wmnet {0} -e "{1}"'.format(db, query)
    if filename:
        cmd = cmd + " > " + filename
    if verbose:
        print(cmd)
    ret = os.system(cmd)
    return ret


def user_hash(client_ip_str, user_agent_str, key):
    conc = client_ip_str + user_agent_str + key
    return hashlib.sha512(conc.encode('utf-8')).hexdigest()


def download_dump_file(file_url, output_file_path, verbose=True):
    output_dir = os.path.dirname(output_file_path)
    if not os.path.isdir(output_dir):
        print("{0} directory does not exist. Create and retry.".format(output_dir))
        return -1
    cmd = 'wget "{0}" -O "{1}"'.format(file_url, output_file_path)
    if verbose:
        print(cmd)
    ret = os.system(cmd)
    return ret

def read_redirects(lang, redirect_dir):
    redirect_dict = {}
    redirect_fn = os.path.join(redirect_dir, "{0}_redirect.tsv".format(lang))
    if os.path.exists(redirect_fn):
        with open(redirect_fn, "r") as f:
            for line in f:
                tokens = line.split("\t")
                source = tokens[0].strip()
                if source.startswith('"') and source.endswith('"'):
                    source = source[1:-1]
                target = tokens[1].strip()
                if target.startswith('"') and target.endswith('"'):
                    target = target[1:-1]
                redirect_dict[source] = target
    else:
        print("{0} does not exist. No redirects taken into consideration.".format(redirect_fn))
    return redirect_dict