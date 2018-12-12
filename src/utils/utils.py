import hashlib
import os


def exec_hive_stat2(query, filename = None, priority = False, verbose = True, nice=False):
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


def exec_mariadb_stat2(query, db, filename = None, verbose = True):
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