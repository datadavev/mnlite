import os, sys
import getopt
import time

from opersist import OPersist
from opersist.cli import getOpersistInstance

from mnonboard import utils
from mnonboard import info_chx
from mnonboard import data_chx
from mnonboard import cn
from mnonboard.defs import CFG, HELP_TEXT, SO_SRVR, CN_SRVR, CN_SRVR_BASEURL, CN_CERT_LOC, APPROVE_SCRIPT_LOC
from mnonboard import default_json, L

def run(cfg):
    """
    Wrapper around opersist that simplifies the process of onboarding a new
    member node to DataONE.

    :param dict cfg: Dict containing config variables
    """
    # auth
    if not cfg['token']:
        cfg['token'] = os.environ.get('D1_AUTH_TOKEN')
    if not cfg['token']:
        print('Your DataONE auth token is missing. Please enter it here and/or store it in the env variable "D1_AUTH_TOKEN".')
        cfg['token'] = info_chx.req_input('Please enter your DataONE authentication token: ')
        os.environ['D1_AUTH_TOKEN'] = cfg['token']
    cfg['cert_loc'] = CN_CERT_LOC[cfg['mode']]
    client = cn.init_client(cn_url=cfg['cn_url'], auth_token=cfg['token'])
    if cfg['info'] == 'user':
        # do the full user-driven info gathering process
        ufields = info_chx.user_input()
        fields = info_chx.transfer_info(ufields)
    else:
        # grab the info from a json
        fields = utils.load_json(cfg['json_file'])
        info_chx.input_test(fields)
        # still need to ask the user for some names
    # now we're cooking
    # get the node path using the end of the path in the 'node_id' field
    end_node_subj = fields['node']['node_id'].split(':')[-1]
    loc = utils.node_path(nodedir=end_node_subj)
    # initialize a repository there (step 5)
    utils.init_repo(loc)
    # write a default settings file in the repo directory
    utils.write_settings(loc)
    names = {}
    for f in ('default_owner', 'default_submitter', 'contact_subject'):
        # add a subject for owner and submitter (may not be necessary if they exist already)
        # add subject for technical contact (step 6)
        val = fields[f] if f not in 'contact_subject' else fields['node'][f]
        name = cn.get_or_create_subj(loc=loc, value=val, client=client, title=f)
        # store this for a few steps later
        names[val] = name
    # set the update schedule and set the state to up
    fields['node']['schedule'] = utils.set_schedule()
    fields['node']['state'] = 'up'
    # okay, now overwrite the default node.json with our new one (step 8)
    utils.save_json(loc=os.path.join(loc, 'node.json'), jf=fields)
    # add node as a subject (step 7) 
    cn.get_or_create_subj(loc=loc, value=fields['node']['node_id'],
                             client=client, title='node',
                             name=end_node_subj)
    # restart the mnlite process to pick up the new node.json (step 9)
    utils.restart_mnlite()
    # run scrapy to harvest metadata (step 10)
    if not cfg['local']:
        utils.harvest_data(loc, end_node_subj)
    # now run tests
    data_chx.test_mdata(loc, num_tests=cfg['check_files'])
    # create xml to upload for validation (step 15)
    files = utils.create_names_xml(loc, node_id=fields['node']['node_id'], names=names)
    # uploading xml (proceed to step 14 and ssh to find xml in ~/d1_xml)
    ssh, work_dir, node_id = utils.start_ssh(server=cfg['cn_url'],
                                             node_id=fields['node']['node_id'],
                                             loc=loc,
                                             ssh=cfg['ssh'])
    time.sleep(0.5)
    utils.upload_xml(ssh=ssh, server=CN_SRVR[cfg['mode']], files=files, node_id=node_id, loc=loc)
    # create and validate the subject in the accounts service (step 16)
    utils.create_subj_in_acct_svc(ssh=ssh, cert=cfg['cert_loc'], files=files, cn=cfg['cn_url'], loc=loc)
    utils.validate_subj_in_acct_svc(ssh=ssh, cert=cfg['cert_loc'], names=names, cn=cfg['cn_url'], loc=loc)
    # download the node capabilities and register the node
    node_filename = utils.dl_node_capabilities(ssh=ssh, baseurl=SO_SRVR[cfg['mode']], node_id=node_id, loc=loc)
    utils.register_node(ssh=ssh, cert=cfg['cert_loc'], node_filename=node_filename, cn=cfg['cn_url'], loc=loc)
    utils.approve_node(ssh=ssh, script_loc=APPROVE_SCRIPT_LOC, loc=loc)
    # close connection
    ssh.close() if ssh else None


def check_chains(cfg, sids):
    """
    Check the version chains of the metadata records on the CN indicated in the config.
    If a file is provided, check the version chains of the SIDs in the file.

    :param dict cfg: Dict containing config variables
    :param list sids: List of SIDs to check the version chains of
    """
    # auth
    if not cfg['token']:
        cfg['token'] = os.environ.get('D1_AUTH_TOKEN')
    if not cfg['token']:
        print('Your DataONE auth token is missing. Please enter it here and/or store it in the env variable "D1_AUTH_TOKEN".')
        cfg['token'] = info_chx.req_input('Please enter your DataONE authentication token: ')
        os.environ['D1_AUTH_TOKEN'] = cfg['token']
    fields = utils.load_json(cfg['json_file'])
    L.info('Initializing client...')
    client = cn.init_client(cn_url=cfg['cn_url'], auth_token=cfg['token'])
    end_node_subj = fields['node']['node_id'].split(':')[-1]
    L.info(f'Using node {end_node_subj}')
    if f'urn:node:{end_node_subj}' in cn.node_list(client):
        L.info(f'Node {end_node_subj} was found on the CN.')
    else:
        L.error(f'Node {end_node_subj} was not found on the CN. Please register the node before attempting to suture version chains.')
        exit(1)
    loc = utils.node_path(nodedir=end_node_subj)
    L.info(f'Loading OPersist database: {loc}')
    op: OPersist = getOpersistInstance(loc)
    L.info('OPersist database loaded.')
    sids = [sid.strip() for sid in sids]
    numsids = len(sids)
    L.info(f'Checking version chains for {numsids} SIDs.')
    repairs = 0
    num = 1
    for sid in sids:
        numstr = f'{num}/{numsids}'
        if cn.chain_check(sid, op, client, numstr):
            repairs += 1
        else:
            L.info(f'No repairs for {sid}.')
        num += 1
    L.info(f'Repairs completed: {repairs}. Closing connections...')
    op.close()
    client._session.close()
    L.info('Done.')


def main():
    """
    Uses getopt to set config values in order to call
    :py:func:`mnlite.mnonboard.cli.run`.

    :returns: Config variable dict to use in :py:func:`mnlite.mnonboard.cli.run`
    :rtype: dict
    """
    # get arguments
    chain_check = False
    try:
        opts = getopt.getopt(sys.argv[1:], 'hiPvLd:l:c:C:',
            ['help', 'init', 'production', 'verbose', 'local' 'dump=', 'load=', 'check=', 'chain-check=']
            )[0]
    except Exception as e:
        L.error('Error: %s' % e)
        print(HELP_TEXT)
        exit(1)
    for o, a in opts:
        if o in ('-h', '--help'):
            # help
            print(HELP_TEXT)
            exit(0)
        if o in ('-i', '--init'):
            # do data gathering
            CFG['info'] = 'user'
        if o in ('-P', '--production'):
            # production case
            CFG['cn_url'] = CN_SRVR_BASEURL % CN_SRVR['production']
            CFG['mode'] = 'production'
        else:
            # testing case
            CFG['cn_url'] = CN_SRVR_BASEURL % CN_SRVR['testing']
            CFG['mode'] = 'testing'
        if o in ('-d', '--dump'):
            # dump default json to file
            utils.save_json(a, default_json())
            exit(0)
        if o in ('-l', '--load'):
            # load from json file
            CFG['info'] = 'json'
            CFG['json_file'] = a
        if o in ('-c', '--check'):
            try:
                CFG['check_files'] = int(a)
            except ValueError:
                if a == 'all': # this should probably not be used unless necessary!
                    CFG['check_files'] = a
                else:
                    L.error('Option -c (--check) requires an integer number of files to check.')
                    print(HELP_TEXT)
                    exit(1)
        if o in ('-S', '--sync-content'):
            CFG['local'] = False
            L.info('Syncing content (-S) will scrape the remote site for new metadata (-L overrides this option).')
        if o in ('-L', '--local'):
            CFG['local'] = True
            L.info('Local mode (-L) will not scrape the remote site and will only test local files.')
        if o in ('-C', '--chain-check'):
            # Chain check (-C) will attempt to repair the version chains of the metadata records on the CN indicated in the config
            # Providing "all" as the argument will attempt to repair the version chains of all SIDs on the specified node in the CN
            # Providing a file with SID strings separated by newlines will attempt to repair those version chains
            chain_check = True
            sids = []
            try:
                with open(a, 'r') as f:
                    sids = f.readlines()
                L.info(f'SID list length: {len(sids)}.')
            except FileNotFoundError:
                L.error('File %s not found.' % a)
                exit(1)
    L.info('running mnonboard in %s mode.\n\
data gathering from: %s\n\
cn_url: %s\n\
metadata files to check: %s' % (CFG['mode'],
                                CFG['info'],
                                CFG['cn_url'],
                                CFG['check_files']))
    try:
        if chain_check:
            check_chains(CFG, sids)
        else:
            run(CFG)
    except KeyboardInterrupt:
        print()
        L.error('Caught KeyboardInterrupt, quitting...')
        exit(1)

if __name__ == '__main__':
    main()
