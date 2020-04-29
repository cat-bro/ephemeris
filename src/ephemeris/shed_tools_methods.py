import time

from bioblend.toolshed import ToolShedInstance

from galaxy import util

from galaxy.tool_util.verify.interactor import (
    ToolTestDescription,
    _handle_def_errors,
    stage_data_in_history,
    RunToolException,
    _verify_outputs,
    JobOutputsError,
)

DEFAULT_TOOL_TEST_WAIT = 86400

VALID_KEYS = [
    "name",
    "owner",
    "changeset_revision",
    "tool_panel_section_id",
    "tool_panel_section_label",
    "tool_shed_url",
    "install_repository_dependencies",
    "install_resolver_dependencies",
    "install_tool_dependencies"
]


def complete_repo_information(tool,
                              default_toolshed_url,
                              require_tool_panel_info,
                              default_install_tool_dependencies,
                              default_install_repository_dependencies,
                              default_install_resolver_dependencies,
                              force_latest_revision):
    repo = dict()
    # We need those values. Throw a KeyError when not present
    repo['name'] = tool['name']
    repo['owner'] = tool['owner']
    repo['tool_panel_section_id'] = tool.get('tool_panel_section_id')
    repo['tool_panel_section_label'] = tool.get('tool_panel_section_label')
    if require_tool_panel_info and repo['tool_panel_section_id'] is None and repo[
            'tool_panel_section_label'] is None and 'data_manager' not in repo.get('name'):
        raise KeyError("Either tool_panel_section_id or tool_panel_section_name must be defined for tool '{0}'.".format(
            repo.get('name')))
    repo['tool_shed_url'] = format_tool_shed_url(tool.get('tool_shed_url', default_toolshed_url))
    repo['changeset_revision'] = tool.get('changeset_revision')
    repo = get_changeset_revisions(repo, force_latest_revision)
    repo['install_repository_dependencies'] = tool.get('install_repository_dependencies',
                                                       default_install_repository_dependencies)
    repo['install_resolver_dependencies'] = tool.get('install_resolver_dependencies',
                                                     default_install_resolver_dependencies)
    repo['install_tool_dependencies'] = tool.get('install_tool_dependencies', default_install_tool_dependencies)
    return repo


def format_tool_shed_url(tool_shed_url):
    formatted_tool_shed_url = tool_shed_url
    if not formatted_tool_shed_url.endswith('/'):
        formatted_tool_shed_url += '/'
    if not formatted_tool_shed_url.startswith('http'):
        formatted_tool_shed_url = 'https://' + formatted_tool_shed_url
    return formatted_tool_shed_url


def get_changeset_revisions(repository, force_latest_revision=False):
    """
    Select the correct changeset revision for a repository,
    and make sure the repository exists
    (i.e a request to the tool shed with name and owner returns a list of revisions).
    Return repository or None, if the repository could not be found on the specified tool shed.
    """
    # Do not connect to the internet when not necessary
    if repository.get('changeset_revision') is None or force_latest_revision:
        ts = ToolShedInstance(url=repository['tool_shed_url'])
        # Get the set revision or set it to the latest installable revision
        installable_revisions = ts.repositories.get_ordered_installable_revisions(repository['name'],
                                                                                  repository['owner'])
        if not installable_revisions:  #
            raise LookupError("Repo does not exist in tool shed: {0}".format(repository))
        repository['changeset_revision'] = installable_revisions[-1]

    return repository


def flatten_repo_info(repositories):
    """
    Flatten the dict containing info about what tools to install.
    The tool definition YAML file allows multiple revisions to be listed for
    the same tool. To enable simple, iterative processing of the info in this
    script, flatten the `tools_info` list to include one entry per tool revision.

    :type repositories: list of dicts
    :param repositories: Each dict in this list should contain info about a tool.
    :rtype: list of dicts
    :return: Return a list of dicts that correspond to the input argument such
             that if an input element contained `revisions` key with multiple
             values, those will be returned as separate list items.
    """

    flattened_list = []
    for repo_info in repositories:
        new_repo_info = dict()
        for key, value in repo_info.items():
            if key in VALID_KEYS:
                new_repo_info[key] = value
        if 'revisions' in repo_info:
            revisions = repo_info.get('revisions', [])
            if not revisions:  # Revisions are empty list or None
                flattened_list.append(new_repo_info)
            else:
                for revision in revisions:
                    # A new dictionary must be created, otherwise there will
                    # be aliasing of dictionaries. Which leads to multiple
                    # repos with the same revision in the end result.
                    new_revision_dict = dict(**new_repo_info)
                    new_revision_dict['changeset_revision'] = revision
                    flattened_list.append(new_revision_dict)
        else:  # Revision was not defined at all
            flattened_list.append(new_repo_info)
    return flattened_list


def verify_tool_keep_history(
    tool_id,
    galaxy_interactor,
    resource_parameters=None,
    register_job_data=None,
    test_index=0,
    tool_version=None,
    quiet=False,
    test_history=None,
    force_path_paste=False,
    maxseconds=DEFAULT_TOOL_TEST_WAIT,
    tool_test_dicts=None
):
    if resource_parameters is None:
        resource_parameters = {}
    tool_test_dicts = tool_test_dicts or galaxy_interactor.get_tool_tests(tool_id, tool_version=tool_version)
    tool_test_dict = tool_test_dicts[test_index]
    tool_test_dict.setdefault('maxseconds', maxseconds)
    testdef = ToolTestDescription(tool_test_dict)
    _handle_def_errors(testdef)

    if test_history is None:
        test_history = galaxy_interactor.new_history()

    # Upload data to test_history, run the tool and check the outputs - record
    # API input, job info, tool run exception, as well as exceptions related to
    # job output checking and register they with the test plugin so it can
    # record structured information.
    tool_inputs = None
    job_stdio = None
    job_output_exceptions = None
    tool_execution_exception = None
    expected_failure_occurred = False
    begin_time = time.time()
    try:
        stage_data_in_history(galaxy_interactor,
                            tool_id,
                            testdef.test_data(),
                            history=test_history,
                            force_path_paste=force_path_paste,
                            maxseconds=maxseconds)
        try:
            tool_response = galaxy_interactor.run_tool(testdef, test_history, resource_parameters=resource_parameters)
            data_list, jobs, tool_inputs = tool_response.outputs, tool_response.jobs, tool_response.inputs
            data_collection_list = tool_response.output_collections
        except RunToolException as e:
            tool_inputs = e.inputs
            tool_execution_exception = e
            if not testdef.expect_failure:
                raise e
            else:
                expected_failure_occurred = True
        except Exception as e:
            tool_execution_exception = e
            raise e

        if not expected_failure_occurred:
            assert data_list or data_collection_list

            try:
                job_stdio = _verify_outputs(testdef, test_history, jobs, tool_id, data_list, data_collection_list, galaxy_interactor, quiet=quiet)
            except JobOutputsError as e:
                job_stdio = e.job_stdio
                job_output_exceptions = e.output_exceptions
                raise e
            except Exception as e:
                job_output_exceptions = [e]
                raise e
    finally:
        if register_job_data is not None:
            end_time = time.time()
            job_data = {
                "tool_id": tool_id,
                "tool_version": tool_version,
                "test_index": test_index,
                "time_seconds": end_time - begin_time,
            }
            if tool_inputs is not None:
                job_data["inputs"] = tool_inputs
            if job_stdio is not None:
                job_data["job"] = job_stdio
            status = "success"
            if job_output_exceptions:
                job_data["output_problems"] = [util.unicodify(_) for _ in job_output_exceptions]
                status = "failure"
            if tool_execution_exception:
                job_data["execution_problem"] = util.unicodify(tool_execution_exception)
                status = "error"
            job_data["status"] = status
            register_job_data(job_data)
    # 
    # galaxy_interactor.delete_history(test_history)
