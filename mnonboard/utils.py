import os
import json
import subprocess
from paramiko import SSHClient
from scp import SCPClient
import urllib.parse as urlparse
import xmltodict
from pathlib import Path
from logging import getLogger

from mnonboard.defs import SCHEDULES, NAMES_DICT, USER_NAME, DEFAULT_SETTINGS
from mnonboard import NODE_PATH_REL, CUR_PATH_ABS, LOG_DIR, HARVEST_LOG_NAME, HM_DATE, L
from mnonboard.info_chx import enter_schedule

def load_json(loc: str):
    """
    Load json from file.

    :param str loc: File location of the json file to be loaded
    :returns: Serialized json file contents
    :rtype: dict
    """
    L.info('Loading member node json from %s' % loc)
    try:
        with open(loc, 'r') as f:
            j = json.load(f)
            L.info('File loaded from %s' % loc)
            return j
    except FileNotFoundError as e:
        L.error('File does not exist - %s' % e)
        exit(1)
    except Exception as e:
        L.error('Error: %s' % e)
        exit(1)

def save_json(loc: str, jf: dict):
    """
    Output json to file.

    :param str loc: File location where the json file is to be written
    :param dict jf: Dictionary to be written as json
    """
    L.info('Writing member node json to %s' % loc)
    try:
        with open(loc, 'w') as f:
            json.dump(jf, f, indent=4)
            L.info('File written to %s' % loc)
            return
    except FileExistsError as e:
        L.error('File exists - %s' % e)
        exit(1)
    except Exception as e:
        L.error('Error: %s' % e)
        exit(1)

def save_report(rep_str: str, loc: str, format: str='.csv'):
    """
    Output a validation report for a set of metadata.

    :param str rep_str: Report string to be written
    :param str loc: File location where the report file is to be written
    :param str jf: File extension (default: .csv)
    """
    fn = os.path.join(loc, 'report-%s%s' % (HM_DATE, format))
    L.info('Writing report to %s' % (fn))
    with open(fn, 'w') as f:
        f.write(rep_str)
    L.info('Done.')

def dumps_json(js):
    """
    Quick and dirty way to output formatted json.

    :param dict js: Dictionary to be written as json
    """
    print(json.dumps(js, indent=2))

def node_path(nodepath: str=NODE_PATH_REL, curpath: str=CUR_PATH_ABS, nodedir: str=''):
    """
    Get the absolute path of the nodes directory where new members will go.
    Currently the nodes directory lives at `../instance/nodes/` (relative to
    the mnonboard dir that this file is in).

    :param str nodepath: Location of the nodes directory relative to the project directory (default: 'instance/nodes/')
    :param str curpath: Current absolute path of this function (default: os.path.dirname(os.path.abspath(__file__)))
    :param str nodedir: Name of the node directory (example: 'HAKAI_IYS'; default: '')
    :returns: Absolute path of the node directory
    :rtype: str
    """
    return os.path.abspath(os.path.join(curpath, '../', nodepath, nodedir))

def init_repo(loc: str):
    '''
    Initialize a new instance using opersist.

    :param str loc: Location of the member node directory in which to initialize an opersist instance
    '''
    try:
        L.info('Using opersist to init new member node folder: %s' % loc)
        subprocess.run(['opersist',
                        '--folder=%s' % (loc),
                        'init'], check=True)
    except Exception as e:
        L.error('opersist init command failed (node folder: %s): %s' % (loc, e))
        exit(1)

def write_settings(loc: str, settings: dict=DEFAULT_SETTINGS):
    """
    Write settings to a file.

    :param str loc: Location of the settings file to be written
    :param dict settings: Dictionary of settings to be written (default: DEFAULT_SETTINGS)
    """
    L.info('Writing settings to %s' % loc)
    sf = Path(loc/'settings.json')
    try:
        with open(str(sf), 'x') as f:
            json.dump(settings, f, indent=4)
        L.info('Settings written to %s' % sf)
    except FileExistsError as e:
        L.warning('File exists - %s' % e)
        try:
            sf = Path(loc/'settings-default.json')
            with open(str(sf), 'w') as f:
                json.dump(settings, f, indent=4)
            L.info('Default settings written to %s' % sf)
        except Exception as e:
            L.error('Error writing default settings to %s: %s' % (sf, e))
            exit(1)
    except Exception as e:
        L.error('Error writing settings to %s: %s' % (sf, e))
        exit(1)

def parse_name(fullname: str):
    """
    Parse full names into given and family designations.

    This function parses full names into given and family names. It supports
    various formats of names, including those with multiple given names and
    family names.

    Supported formats:
    Multiple given names: ``John Jacob Jingleheimer Schmidt``
    Given name and family name: ``John Schmidt``
    Family name and given name: ``Schmidt, John``
    Given name and family name with prefix: ``John von Schmidt``

    :param fullname: The full name to be parsed.
    :type fullname: str
    :return: A tuple containing the given name and family name.
    :rtype: tuple[str, str]
    """
    given, family = None, None
    if ', ' in fullname:
        # split the fullname by comma and space, assign the family name and given name
        [family, given] = fullname.title().split(', ')[:2]
    if (given == None) and (family == None):
        for q in [' del ', ' van ', ' de ', ' von ', ' der ', ' di ', ' la ', ' le ', ' da ', ' el ', ' al ', ' bin ']:
            if q in fullname.lower():
                # split the fullname by the query string, assign the given name and family name
                [given, family] = fullname.lower().split(q)
                # capitalize the and concat the query string to the family name
                given = given.title()
                family = f'{q.strip()}{family.title()}'
    if (given == None) and (family == None):
        # split the fullname by space and capitalize each part
        nlist = fullname.title().split()
        # assign the last part as the family name and the first part as the given name
        family = nlist[-1]
        if len(nlist) >= 2:
            given = nlist[0]
            for i in range(1, len(nlist)-1):
                # concatenate the remaining parts as the given name
                given = f'{given} {nlist[i]}'
    if (not given) or (not family):
        L = getLogger(__name__)
        L.warning(f'Could not parse name "{fullname}". Result of given name: "{given}" Family name: "{family}"')
    return given, family

def new_subj(loc: str, name: str, value: str):
    """
    Create new subject in the database using opersist.

    :param str loc: Location of the opersist instance
    :param str name: Subject name (human readable)
    :param str value: Subject value (unique subject id, such as orcid or member node id)
    """
    try:
        L.info('opersist creating new subject. Name: %s Value: %s Location: %s' % (name, value, loc))
        subprocess.run(['opersist',
                        '--folder=%s' % (loc),
                        'sub',
                        '--operation=create',
                        '--name=%s' % name,
                        '--subj=%s' % value], check=True)
    except Exception as e:
        L.error('opersist subject creation command failed for %s (%s): %s' % (name, value, e))
        exit(1)

def set_schedule():
    """
    Ask the user what schedule on which they would like to run scrapes.
    Options are: monthly, daily, and every 3 minutes.

    :returns: Dictionary entry formatted based on the chosen schedule option
    :rtype: dict
    """
    s = enter_schedule()
    return SCHEDULES[s]

def restart_mnlite():
    """
    Subprocess call to restart the mnlite system service. Requires sudo.
    """
    while True:
        i = input('Do you wish to restart the mnlite service? (Y/n) ')
        if i.lower() == 'n':
            break
        elif i.lower() in ['y', '']:
            L.info('Restarting mnlite systemctl service...')
            try:
                subprocess.run(['sudo', 'systemctl', 'restart', 'mnlite.service'], check=True)
                L.info('Done.')
                break
            except subprocess.CalledProcessError as e:
                L.error('Error restarting mnlite system service. Is it installed on your system? Error text:\n%s' % (e))
                print('mnlite was not restarted.')
                i = input('Do you wish to continue? (Y/n) ')
                if i.lower() == 'n':
                    L.info('User has chosen to abort setup after mnlite restart failed.')
                    exit(1)
                elif i.lower() in ['y', '']:
                    L.info('User has chosen to continue after mnlite restart failed.')
                    break
                else:
                    L.error('Invalid input at mnlite failure continue prompt: %s' % (i))
                    print('You have selected an invalid option.')
        else:
            L.error('Invalid input at mnlite prompt: %s' % (i))
            print('You have selected an invalid option.')


def harvest_data(loc: str, mn_name: str):
    """
    Use scrapy to harvest data to the specified path, and output a log to the
    specified location.

    :param str loc: Location of the opersist instance
    :param str mn_name: Name of the member node (used to name the crawl log)
    """
    log_loc = os.path.join(LOG_DIR, mn_name + HARVEST_LOG_NAME)
    L.info('Starting scrapy crawl, saving to %s' % (loc))
    L.info('scrapy log location is %s' % (log_loc))
    try:
        subprocess.run(['scrapy', 'crawl', 'JsonldSpider',
                        '--set=STORE_PATH=%s' % loc,
                        '--logfile=%s' % log_loc],
                        check=True)
        L.info('scrapy crawl complete.')
    except Exception as e:
        L.error('Error running scrapy: %s' % e)

def limit_tests(num_things: int):
    """
    Ask the user to limit the number of tests to run on a given set of
    metadata.
    This will execute if the user decides to try and test more than 500
    metadata objects.
    The prompt will ask them if they wish to limit the number, then return
    a number based on their decision.

    :param int num_things: Initial number of things to test
    :returns: Modified number of things to test
    :rtype: int
    """
    while True:
        i = input('Testing more than 500 objects is not recommended due to performance concerns.\n\
This may take several minutes and use critical server resources. (est: %s min)\n\
Are you sure you want to test all %s metadata objects in this set? (y/N): ' % (round(num_things/500), num_things))
        if (i.lower() in 'n') or (i.lower() in ''):
            L.info('User has chosen enter a new number of objects to test.')
            while True:
                n = input('Please enter a new number of metadata objects to test: ')
                try:
                    num_things = int(n)
                    break
                except ValueError as e:
                    L.error('User has not entered a number ("%s")' % n)
            if num_things <= 500:
                L.info('User has chosen to test %s metadata objects.' % num_things)
                break
        else:
            L.info('User has chosen to continue testing %s objects.' % (num_things))
            break
    return num_things

def ask_continue(msg: str):
    """
    A user input loop in which the user is prompted whether they want to
    continue.

    :param str msg: The message to display at the prompt
    """
    while True:
        i = input(msg + ' (Y/n) ')
        if i.lower() in 'y':
            # this also implicitly matches an empty string (i.e. user presses enter)
            L.info('Continuing.')
            break
        elif (i.lower() in 'n'):
            L.info('Exiting.')
            exit(1)
        else:
            L.info('User has not entered "y" or "n".')
            print('You have entered an incorrect value. Please enter "y" to continue or "n" to quit.')
            continue

def create_names_xml(loc: str, node_id: str, names: dict):
    """
    Format subject XML documents and return list of names.

    :param str loc: Location (dir) to write file to
    :param str node_id: Node id of current MN
    :param dict names: Dict of subject names with ORCiD as index

    :returns: List of files written
    :rtype: list
    """
    # make dir
    loc = os.path.join(loc, 'xml')
    try:
        os.makedirs(loc, exist_ok=True)
    except OSError as e:
        L.error('OSError creating XML directory: %s' % (e))
        exit(1)
    except Exception as e:
        L.error('%s creating XML directory: %s' % (repr(e), e))
        exit(1)
    # format NAMES_XML
    node_id = node_id.split(':')[-1]
    files = []
    for id in names:
        namesplit = names[id].split()
        first, last = namesplit[0], namesplit[-1]
        xd = NAMES_DICT
        xd['ns2:person']['subject'] = id
        xd['ns2:person']['givenName'] = first
        xd['ns2:person']['familyName'] = last
        fn = os.path.join(loc, '%s_%s%s.xml' % (node_id, first[0], last))
        with open(fn, 'w') as f:
            xmltodict.unparse(xd, output=f)
        L.debug('XML path: %s' % fn)
        files.append(fn)
    return files

def write_cmd_to(fn, cmd, desc=None, mode='a'):
    """
    """
    desc = f"# {desc}\n" if desc else ""
    with open(fn, mode) as f:
        f.write(f"{desc}{cmd}\n")

def start_ssh(server: str, node_id, loc: str, ssh: bool=True):
    """
    Starts ssh client connection to a given server.

    :param str server: The remote CN server to connect to (e.g. ``"cn-stage-ucsb-1.test.dataone.org"``)
    :param str node_id: The node identifier (e.g. ``"urn:node:OPENTOPO"``)
    :param str loc: The local location of the member node directory (e.g. ``"instance/nodes/mnTestOPENTOPO"``)
    :param bool ssh: ``True`` will attempt to establish a remote connection. ``False`` will return values to output command strings to file. Default: True
    """
    server = server.split('https://')[1].split('/')[0]
    node_id = node_id.split(':')[-1]
    xml_dir = '~/d1_xml/%s' % (node_id)
    local_xml_dir = f'{loc}/xml'
    mkdir_cmd = 'mkdir -p %s' % (xml_dir)
    cd_cmd = 'cd %s' % xml_dir
    op = f'connection to {server}'
    if not ssh:
        return None, local_xml_dir, node_id
    try:
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(server)
        L.info('Running "%s" on %s' % (mkdir_cmd, server))
        op = f'mkdir on {server}'
        ssh.exec_command(mkdir_cmd)
        L.info('Running "%s" on %s' % (cd_cmd, server))
        op = f'cd on {server}'
        ssh.exec_command(cd_cmd)
        return ssh, xml_dir, node_id
    except Exception as e:
        L.error('%s running %s. Details: %s' % (repr(e), op, e))
        return None, local_xml_dir, node_id

def upload_xml(ssh: SSHClient, files: list, node_id: str, loc: str, usern: str=USER_NAME, server: str=None):
    """
    Format subject XML documents and return list of names.

    :param paramiko.SSHClient ssh: The SSH client (if one was created) or ``False``
    :type ssh: paramiko.SSHClient or bool
    :param list files: List of files to upload
    :param str node_id: The node identifier (e.g. ``"urn:node:OPENTOPO"``)
    :param str loc: The local location of the member node directory (e.g. ``"instance/nodes/mnTestOPENTOPO"``)
    :param str server: The remote CN server to connect to (e.g. ``"cn-stage-ucsb-1.test.dataone.org"``)
    """
    op = ''
    target_dir = f'~/d1_xml/{node_id}/'
    try:
        op = 'mkdir on remote server'
        if ssh:
            with SCPClient(ssh.get_transport()) as scp:
                op = 'scp to remote server'
                L.info('Copying files to remote %s : %s' % (target_dir, files))
                scp.put(files=files, remote_path=target_dir)
        else:
            cmd_fn = f"{loc}/commands.sh"
            write_cmd_to(fn=cmd_fn, cmd=f'## Commands to be run on the CN:', mode='w')
            write_cmd_to(fn=cmd_fn, cmd=f'mkdir -p {target_dir}', desc='Create and enter dir for xml files')
            write_cmd_to(fn=cmd_fn, cmd=f'cd {target_dir}')
            write_cmd_to(fn=cmd_fn, cmd=f'\n')
            write_cmd_to(fn=cmd_fn, cmd=f'## Run on MN to transfer subject xml files to the CN (make sure username is correct):')
            for f in files:
                command = f"scp {f} {usern}@{server}:{target_dir}"
                write_cmd_to(fn=cmd_fn, cmd=command)
            write_cmd_to(fn=cmd_fn, cmd=f'\n')
    except Exception as e:
        L.error('%s running %s. Details: %s' % (repr(e), op, e))
        exit(1)

def create_subj_in_acct_svc(ssh: SSHClient, cert: str, files: list, cn: str, loc: str):
    """
    Create a subject in the accounts service on the CN.

    :param paramiko.SSHClient ssh: The SSH client (if one was created) or ``False``
    :type ssh: paramiko.SSHClient or bool
    :param str cert: The location of the CN certificate on the remote server
    :param list files: List of XML subject files to upload
    :param str cn: The base https address of the CN to use for API calls (e.g. ``"https://cn-stage.test.dataone.org/cn"``)
    :param str loc: The local location of the member node directory (e.g. ``"instance/nodes/mnTestOPENTOPO"``)
    """
    cmd_fn = f"{loc}/commands.sh"
    for f in files:
        f = os.path.split(f)[1]
        command = 'curl -s --cert %s -F person=@%s -X POST %s/v2/accounts' % (
            cert, f, cn
        )
        if ssh:
            L.info('Creating subject: %s' % (command))
            ssh.exec_command(command)
        else:
            L.debug(f'Command: {command}')
            L.info(f'Writing cmd to {cmd_fn}: subject creation')
            write_cmd_to(fn=cmd_fn, cmd="## Commands to be run by an admin on the CN:")
            write_cmd_to(fn=cmd_fn, cmd=command, desc=f"Create subject: {f}")

def validate_subj_in_acct_svc(ssh: SSHClient, cert: str, names: dict, cn: str, loc: str):
    """
    Validate the subjects created using
    :py:func:`mnlite.mnonboard.utils.create_subj_in_acct_svc`.

    :param paramiko.SSHClient ssh: The SSH client (if one was created) or ``False``
    :type ssh: paramiko.SSHClient or bool
    :param str cert: The location of the CN certificate on the remote server
    :param dict names: Dictionary of names indexed by ORCiD (e.g. ``{"http://orcid.org/0000-0001-5828-6070": "Ian Nesbitt"}``)
    :param str cn: The base https address of the CN to use for API calls (e.g. ``"https://cn-stage.test.dataone.org/cn"``)
    :param str loc: The local location of the member node directory (e.g. ``"instance/nodes/mnTestOPENTOPO"``)
    """
    cmd_fn = f"{loc}/commands.sh"
    for n in names:
        orcid_urlenc = urlparse.quote(n, safe='-')
        command = 'curl -s --cert %s -X PUT %s/v2/accounts/verification/%s' % (
            cert, cn, orcid_urlenc
        )
        if ssh:
            L.info('Validating subject: %s' % (command))
            ssh.exec_command(command)
        else:
            L.debug(f'Command: {command}')
            L.info(f'Writing cmd to {cmd_fn}: subject validation')
            write_cmd_to(fn=cmd_fn, cmd=command, desc=f"Validate subject: {n}")

def dl_node_capabilities(ssh: SSHClient, baseurl: str, node_id: str, loc: str):
    """
    Download the node capabilities xml document from the SO server and save it
    to file.

    :param paramiko.SSHClient ssh: The SSH client (if one was created) or ``False``
    :type ssh: paramiko.SSHClient or bool
    :param str baseurl: The base URL of the schema.org server to download node config from (e.g. ``"so.test.dataone.org"``)
    :param str node_id: The end triplet of the node identifier (e.g. ``"OPENTOPO"``)
    :param str loc: The local location of the member node directory (e.g. ``"instance/nodes/mnTestOPENTOPO"``)
    :returns: Location of the xml filepath where the node doc is saved
    :rtype: str
    """
    cmd_fn = f"{loc}/commands.sh"
    target_dir = f'~/d1_xml/{node_id}'
    node_filename = '%s/%s-node.xml' % (target_dir, node_id)
    command = 'curl "https://%s/%s/v2/node" > %s' % (baseurl, node_id, node_filename)
    if ssh:
        L.info('Downloading node capabilities: %s' % (command))
        ssh.exec_command(command)
    else:
        L.info(f'Writing cmd to {cmd_fn}: node capabilities')
        L.debug(f'Command: {command}')
        write_cmd_to(fn=cmd_fn, cmd=command, desc=f"Download {node_id} node capabilities")
    return node_filename

def register_node(ssh: SSHClient, cert: str, node_filename: str, cn: str, loc: str):
    """
    Registers the node in the CN.

    :param paramiko.SSHClient ssh: The SSH client (if one was created) or ``False``
    :type ssh: paramiko.SSHClient or bool
    :param str node_filename: The xml filepath returned by :py:func:`mnlite.mnonboard.utils.dl_node_capabilities`
    :param str loc: The local location of the member node directory (e.g. ``"instance/nodes/mnTestOPENTOPO"``)
    """
    cmd_fn = f"{loc}/commands.sh"
    node_filename = os.path.split(node_filename)[1]
    mn = node_filename.split('-')[0]
    command = """curl --cert %s -X POST -F 'node=@%s' "%s/v2/node" """ % (
        cert, node_filename, cn
    )
    if ssh:
        L.info('Registering node: %s' % (command))
        ssh.exec_command(command)
    else:
        L.info(f'Writing cmd to {cmd_fn}: node registration')
        L.debug(f'Command: {command}')
        write_cmd_to(fn=cmd_fn, cmd=command, desc=f"Register {node_filename} with CN")

def approve_node(ssh: SSHClient, script_loc: str, loc: str):
    """
    Starting the node approval script.

    .. warning:: This does not work over SSH!

    .. note:: 

        In the future, this will be deprecated in favor of a method that does
        not use Hazelcast.

    :param paramiko.SSHClient ssh: The SSH client (if one was created) or ``False``
    :type ssh: paramiko.SSHClient or bool
    :param str script_loc: The location of the node approval script (e.g. ``"/usr/local/bin/dataone-approve-node"``)
    :param str loc: The local location of the member node directory (e.g. ``"instance/nodes/mnTestOPENTOPO"``)
    """
    cmd_fn = f"{loc}/commands.sh"
    command = 'sudo %s' % (script_loc)
    if ssh:
        L.info('Starting approval script: %s' % (command))
        ssh.exec_command(command)
    else:
        L.info(f'Writing to {cmd_fn}: node approval')
        write_cmd_to(fn=cmd_fn, cmd=command, desc="Approve node with CN (interactive script)")
