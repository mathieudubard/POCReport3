from glob import glob as gg
import json
import logging
import os
import pandas as pd
import shutil
import tempfile
import zipfile
import requests
import time
from json import JSONDecodeError
import urllib3
import boto3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

DEFAULT_SETTINGS = ['inputFileName', 'outputFormat', 'outputPaths', 'inputPath', 'logPath']


class ModelRunParameters :
    def __init__(self, model_run_paramter_json, file_path, credentials) :
        self.path = file_path
        self.json = model_run_paramter_json
        self.name = model_run_paramter_json.get('name')
        self.model_factors = model_run_paramter_json['datasets'].get('modelFactors', [])
        self.input_data = {data['category'] : data['attributes'] for data in model_run_paramter_json['datasets'].get('inputData', {})}
        self.output_data = {data['category'] : data['attributes'] for data in model_run_paramter_json['datasets'].get('outputData', {})}
        self.supporting_data = {data['category'] : data['attributes'] for data in model_run_paramter_json['datasets'].get('supportingData', {})}
        self.scenarios = model_run_paramter_json['settings'].get('scenarios', [])
        self.output_s3_paths = model_run_paramter_json['settings']['outputPaths']
        self.input_s3_path = model_run_paramter_json['settings']['inputPath']
        self.output_s3_path = f'{"/".join((self.input_s3_path).split("/")[:-1])}/output'
        self.log_s3_path = model_run_paramter_json['settings']['logPath']
        self.run_date = pd.to_datetime(model_run_paramter_json['settings']['runDate'])
        self.reporting_date = pd.to_datetime(model_run_paramter_json['settings']['reportingDate'])
        self.model_as_of_date = self._getModelAsOfDate()
        self.settings = model_run_paramter_json.get('settings', {})
        self.model_settings = model_run_paramter_json['datasets'].get('settings', [])
        self.callBack = self._set_additional_settings(model_run_paramter_json, credentials)
        self.json.update({'settings' : self.settings})

    def _set_additional_settings(self, model_run_parameter_json, credentials) :
        settingsCallbackUrlParam = next((s for s in self.model_settings if s == 'settingsCallbackUrl'), None)
        settingsCallbackUrl = model_run_parameter_json.get('settings', {}).get('settingsCallbackUrl')
        if settingsCallbackUrl:
            print("[callback] url present, param in model_settings={}".format(settingsCallbackUrlParam is not None))
        res = False
        # Run callback when URL is present (even if wrapper omits settingsCallbackUrl from datasets.settings)
        if settingsCallbackUrl :
            response = requests.request('POST',
                                        url=settingsCallbackUrl,
                                        headers={'Authorization' : "Bearer " + credentials.get('jwt')},
                                        json=model_run_parameter_json)
            log.info(f'Response status code: {response.status_code}')
            if response.status_code == 200 :
                try :
                    new_settings = json.loads(response.text).get('settings', {})
                    for key in new_settings :
                        if key not in DEFAULT_SETTINGS :
                            self.settings[key] = new_settings[key]
                    IOSession.normalize_analyses_to_settings(self.settings)
                    n_ids = len(self.settings.get('analysisIds') or [])
                    print("[callback] merged settings, analysisIds count={}".format(n_ids))
                    res = True
                except JSONDecodeError as e :
                    log.error(f'Cannot deserialize: {response.text}')
                    raise RuntimeError(f"Cannot deserialize: {e}")
            else :
                log.error(f'Response: {response.text}')
                raise RuntimeError(f"Unexpected response from {settingsCallbackUrl}")
        elif settingsCallbackUrlParam and not settingsCallbackUrl :
            raise RuntimeError("Model parameters do not contain required settingsCallbackUrl")
        else :
            log.info('No additional settings')
        return res

    def _getModelAsOfDate(self) :
        try :
            return pd.to_datetime(self.json['settings']['modelAsOfDate'])
        except :
            return None


class IOSession :
    def __init__(self, cap_session, s3_json_key, local_mode, credentials) :
        self.logger = logging.getLogger(__name__)
        self.local_mode = local_mode
        self.cap_session = cap_session

        self.local_temp_directory = tempfile.mkdtemp().replace('\\', '/')
        print("[IOSession] temp_dir={}".format(self.local_temp_directory))
        self.logger.debug(f'Created local temp directory: {self.local_temp_directory}')
        self.model_run_parameters = self.getModelRunParameters(s3_json_key, credentials)
        self.local_directories = self.create_io_directories()
        # self.local_dir = self.create_addtl_io_directories()

        if local_mode :
            test_folder = os.path.abspath(os.path.split(s3_json_key)[0]).replace('\\', '/')
            self.input_path = f'{test_folder}/input_csv'
            self.test_folder_output = f'{test_folder}/output'
            self.initializeDirectory(self.test_folder_output)
        else :
            self.input_path = self.model_run_parameters.input_s3_path

    def create_io_directories(self) :
        """Create local directories for every input/output/log directory in modelRunParameter.json settings"""
        local_directories = {'inputPath' : {}}
        s3_client = boto3.client('s3')
        s3_resource = boto3.resource('s3')
        bucket = self.cap_session.context['s3_bucket']
        key = self.model_run_parameters.input_s3_path
        resp = self.cap_session.init_s3_client().list_objects_v2(Bucket=bucket, Prefix=key)
        cont = resp.get('Contents') or []  # empty prefix returns no 'Contents' key; optional payload files
        input_path_list = []  # This is to update the local mrp.json - a list of dicts of temp dir and file under each dir
        test_list = []  # This is the list of input files to be written to the actual mrp.json
        dir_list = []  # This is to write the directories locally
        for elem in cont:
            # print(elem)
            tempPath = elem['Key']  # This is the remote path from S3 for the file that needs to be downloaded
            test_list.append(tempPath)
            dirPath = '/'.join((tempPath.split("/")[:-1]))
            localDirPath = dirPath.replace(key, f'{self.local_temp_directory}/input')  # This is the local dirPath
            if localDirPath not in dir_list :
                self.initializeDirectory(localDirPath)
                dir_list.append(localDirPath)
            filePath = tempPath.replace(key, f'{self.local_temp_directory}/input')
            # tempDict = {dirPath : file}
            input_path_list.append(filePath)

        local_directories['inputPath'] = {'custInputs' : input_path_list}
        self.model_run_parameters.settings['inputPath'] = test_list

        local_directories['outputs'] = self.initializeDirectory(f'{self.local_temp_directory}/outputs')
        self.model_run_parameters.settings['outputs'] = f'{key}/outputs'
        local_directories['logPath'] = self.initializeDirectory(f'{self.local_temp_directory}/logPath')

        local_directories['outputPaths'] = {}
        for file in self.model_run_parameters.output_s3_paths :
            path = self.initializeDirectory(f'{self.local_temp_directory}/outputPaths/{file}')
            local_directories['outputPaths'].update({file : path})

        print("[create_io_directories] callBack={}, payload_files={}".format(
            self.model_run_parameters.callBack, len(cont)))
        if self.model_run_parameters.callBack is True :
            analysis_ids = self.model_run_parameters.settings.get('analysisIds', []) or []
            local_directories['inputPaths'] = {}
            for file1, inputs in self.model_run_parameters.settings['inputPaths'].items() :
                analysis_path = []
                for idx, elem in enumerate(inputs):
                    # Use analysisIds by index so each analysis gets its own dir (elem path may be "output/.../analysisidentifier=4647997/..." where [1] is "instrumentResult", not the id)
                    an_id = analysis_ids[idx] if idx < len(analysis_ids) else elem.split('/')[1]
                    path1 = self.initializeDirectory(f'{self.local_temp_directory}/inputPaths/{an_id}/{file1}')
                    analysis_path.append(path1)
                local_directories['inputPaths'].update({file1 : analysis_path})
            # Ensure instrumentResult, instrumentReporting, instrumentReference, macroEconomicVariableInput dirs (callback may only return some)
            analysis_ids = self.model_run_parameters.settings.get('analysisIds', []) or []
            for cat in ('instrumentResult', 'instrumentReporting', 'instrumentReference', 'macroEconomicVariableInput'):
                if cat not in local_directories['inputPaths']:
                    analysis_path = [self.initializeDirectory(f'{self.local_temp_directory}/inputPaths/{an_id}/{cat}') for an_id in analysis_ids]
                    local_directories['inputPaths'][cat] = analysis_path
        return local_directories

    def list_and_print_s3_folders(self, prefix=None, list_object_keys=True):
        """
        List and print what Cappy finds under the S3 bucket for the given prefix:
        - 'folders' (common prefixes when using Delimiter='/')
        - optionally all object keys under the prefix.
        Only runs when not in local_mode. Uses cap_session.init_s3_client().
        """
        if self.local_mode:
            self.logger.info("S3 list skipped (local mode).")
            return
        bucket = self.cap_session.context.get('s3_bucket')
        if not bucket:
            self.logger.warning("No s3_bucket in cap_session context; cannot list S3.")
            return
        prefix = prefix or self.model_run_parameters.input_s3_path
        client = self.cap_session.init_s3_client()
        print(f"\n--- S3 bucket: {bucket}")
        print(f"--- Prefix: {prefix}\n")
        # List "folders" (common prefixes)
        try:
            resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
            common_prefixes = resp.get('CommonPrefixes') or []
            if common_prefixes:
                print("Folders (common prefixes):")
                for p in common_prefixes:
                    name = p.get('Prefix', '')
                    print(f"  {name}")
            else:
                print("Folders (common prefixes): (none under this prefix)")
            # Optionally list object keys
            if list_object_keys:
                keys = []
                paginator = client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get('Contents') or []:
                        keys.append(obj.get('Key', ''))
                if keys:
                    print(f"\nObject keys ({len(keys)}):")
                    for k in keys[:50]:
                        print(f"  {k}")
                    if len(keys) > 50:
                        print(f"  ... and {len(keys) - 50} more")
                else:
                    print("\nObject keys: (none)")
        except Exception as e:
            self.logger.error(f"Failed to list S3: {e}", exc_info=True)
            print(f"Error listing S3: {e}")
        print("")

    def _list_s3_at_prefix(self, prefix, label="", max_keys=50):
        """
        List and print folders (CommonPrefixes) and object keys at the given S3 prefix.
        Used for progressive S3 diagnostics. Returns True if list succeeded, False on error.
        """
        if self.local_mode:
            return False
        bucket = self.cap_session.context.get('s3_bucket')
        if not bucket:
            return False
        client = self.cap_session.init_s3_client()
        try:
            print(f"\n[S3 list] {label}")
            print(f"  Prefix: {prefix or '(bucket root)'}")
            resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix or '', Delimiter='/', MaxKeys=1000)
            common_prefixes = resp.get('CommonPrefixes') or []
            if common_prefixes:
                print(f"  Folders ({len(common_prefixes)}):")
                for p in common_prefixes[:max_keys]:
                    print(f"    {p.get('Prefix', '')}")
                if len(common_prefixes) > max_keys:
                    print(f"    ... and {len(common_prefixes) - max_keys} more")
            else:
                print("  Folders: (none)")
            contents = resp.get('Contents') or []
            if contents:
                print(f"  Objects ({len(contents)}):")
                for obj in contents[:max_keys]:
                    print(f"    {obj.get('Key', '')}")
                if len(contents) > max_keys:
                    print(f"    ... and {len(contents) - max_keys} more")
            else:
                print("  Objects: (none)")
            return True
        except Exception as e:
            self.logger.error(f"Failed to list S3 at {prefix}: {e}", exc_info=True)
            print(f"  ERROR: {e}")
            return False

    def _get_s3_object_keys(self, prefix):
        """Return list of object keys (files) under the given prefix. Uses pagination."""
        if self.local_mode:
            return []
        bucket = self.cap_session.context.get('s3_bucket')
        if not bucket:
            return []
        keys = []
        try:
            client = self.cap_session.init_s3_client()
            paginator = client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix or '', MaxKeys=1000):
                for obj in (page.get('Contents') or []):
                    k = obj.get('Key')
                    if k:
                        keys.append(k)
        except Exception as e:
            self.logger.debug(f"List objects at {prefix}: {e}")
        return keys

    def _list_s3_under_prefix_recursive(self, prefix, max_depth=5, _current_depth=0, _folders=None, _objects=None):
        """
        Recursively list all folders (common prefixes) and object keys under prefix.
        Respects max_depth to avoid runaway. Returns (list of folder prefixes, list of object keys).
        """
        if self.local_mode:
            return [], []
        bucket = self.cap_session.context.get('s3_bucket')
        if not bucket:
            return [], []
        if _folders is None:
            _folders = []
        if _objects is None:
            _objects = []
        if _current_depth >= max_depth:
            return _folders, _objects
        client = self.cap_session.init_s3_client()
        prefix = (prefix or '').rstrip('/') + '/' if (prefix or '') else ''
        try:
            paginator = client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/', MaxKeys=1000):
                for p in (page.get('CommonPrefixes') or []):
                    sub = (p.get('Prefix') or '').rstrip('/')
                    if sub and sub not in _folders:
                        _folders.append(sub)
                    self._list_s3_under_prefix_recursive(sub + '/', max_depth, _current_depth + 1, _folders, _objects)
                for obj in (page.get('Contents') or []):
                    k = obj.get('Key')
                    if k and k not in _objects:
                        _objects.append(k)
        except Exception as e:
            self.logger.debug(f"List at {prefix}: {e}")
        return _folders, _objects

    def list_s3_execution_tree_and_summary(self, execution_base, max_depth=5):
        """
        List recursively under execution_base (e.g. ...-report/), collect all folders and objects,
        then print a single summary at the end. Use trailing slash on execution_base.
        """
        if self.local_mode or not execution_base:
            return
        base = execution_base.rstrip('/') + '/'
        print("\n[S3 recursive] Walking tree under execution (max_depth={})...".format(max_depth))
        folders, objects = self._list_s3_under_prefix_recursive(base, max_depth=max_depth)
        folders.sort()
        objects.sort()
        print("\n" + "=" * 60)
        print("S3 UNDER EXECUTION: SUMMARY (all paths to reasonable depth)")
        print("=" * 60)
        print("\nFolders (prefixes) found ({}):".format(len(folders)))
        for f in folders:
            print("  ", f)
        if not folders:
            print("  (none)")
        print("\nObjects (keys) found ({}):".format(len(objects)))
        for o in objects:
            print("  ", o)
        if not objects:
            print("  (none)")
        print("=" * 60 + "\n")

    def _downloadFile(self, download_key, local_file_path, raise_on_error=False) :
        file_name = os.path.splitext(os.path.basename(local_file_path))[0]
        try :
            self.cap_session.s3_download_file(download_key, local_file_path)
            self.logger.info(f'Successfully downloaded {download_key} to {local_file_path}')
            return {file_name : local_file_path}
        except Exception as e :
            self.logger.error(f'Error downloading {download_key} to {local_file_path}')
            self.logger.debug(e, exc_info=True)
            if raise_on_error :
                raise
            else :
                return {}

    def _downloadDir(self, download_key, local_dir_path, raise_on_error=False) :
        try :
            # cont = self.cap_session.s3_list_bucket()
            self.cap_session.s3_download_file(download_key, local_dir_path)
            self.logger.info(f'Successfully downloaded {download_key} to {local_dir_path}')
            return {}
        except Exception as e :
            self.logger.error(f'Error downloading {download_key} to {local_dir_path}')
            self.logger.debug(e, exc_info=True)
            if raise_on_error :
                raise
            else :
                return {}

    def deleteTempDirectories(self) :
        def deleteDir(directory) :
            try :
                shutil.rmtree(directory)
                self.logger.info(f'Successfuly deleted temporary directory: {directory}')
            except Exception as e :
                self.logger.error(f'Error deleting temporary directory: {directory}')
                self.logger.debug(e, exc_info=True)

        deleteDir(self.local_temp_directory)

    def _uploadFile(self, local_file_path, upload_key, raise_on_error=False) :
        try :
            self.cap_session.s3_upload_file(local_file_path, upload_key)
            self.logger.info(f'Successfully uploaded {os.path.basename(local_file_path)} to {upload_key}')
        except Exception as e :
            self.logger.error(f'Error uploading {local_file_path} to {upload_key}')
            self.logger.debug(e, exc_info=True)
            if raise_on_error :
                raise

    def initializeDirectory(self, directory) :
        self.logger.debug(f'Clearing directory {directory}')
        shutil.rmtree(directory, ignore_errors=True)
        os.makedirs(directory, exist_ok=True)
        return directory

    def _safeCopyFile(self, from_file, to_file, raise_on_error=False) :
        os.makedirs(os.path.dirname(to_file), exist_ok=True)
        file_name = os.path.splitext(os.path.basename(to_file))[0]
        try :
            shutil.copyfile(from_file, to_file)
            self.logger.info(f'Successfully copied {from_file} to {to_file}')
            return {file_name : to_file}
        except Exception as e :
            self.logger.warning(f'Error copying {from_file} to {to_file}')
            self.logger.debug(e, exc_info=True)
            if raise_on_error :
                raise
            else :
                return {}

    def _safeCopyDir(self, from_dir, to_dir, raise_on_error=False) :
        os.makedirs(to_dir, exist_ok=True)
        try :
            shutil.copy(from_dir, to_dir)
            self.logger.info(f'Successfully copied {from_dir} to {to_dir}')
        except Exception as e :
            self.logger.warning(f'Error copying {from_dir} to {to_dir}')
            self.logger.debug(e, exc_info=True)
            if raise_on_error :
                raise
            else :
                return {}

    def _createFileDict(self, directory) :
        """
        Creates a dictionary of files in a given directory
        :param: directory - path to a directory to create dictionary from
        :return: dictionary of form {file_name_wo_ext: file_path}
        """
        file_dict = {}
        try :
            for file in os.listdir(directory) :
                file_name = os.path.splitext(file)[0]
                file_path = f'{directory}/{file}'
                if os.path.isfile(file_path) :
                    file_dict.update({file_name : file_path})
        except FileNotFoundError as e :
            self.logger.warning(f'Not found: {directory}')
            self.logger.debug(e, exc_info=True)
        return file_dict

    def createFileDicts(self, directory) :
        """
        Creates a dictionary of files in a given directory
        :param: directory - path to a directory to create dictionaries from
        :return: one or more dictionaries of form {file_name_wo_ext: file_path}
        :rtype: list(dict)
        """
        file_dicts = []
        all_paths = gg(f'{os.path.abspath(directory)}{os.sep}**', recursive=True)
        all_files = [path for path in all_paths if os.path.isfile(path)]
        for file in all_files :
            # print(file)
            # file_dict = file_dicts[-1]
            file_dict = {}
            file_name = os.path.splitext(os.path.basename(file))[0]
            file_path = os.path.abspath(file)
            # if file_name in file_dict :
            #     file_dicts.append({})
            #     file_dict = file_dicts[-1]
            file_dict[file_name] = file_path
            file_dicts.append(file_dict)
        return file_dicts if file_dicts != [{}] else []

    def createOutputFileDicts(self, local_model_run_parameters_path=None):
        """
        Build a single dict of only output files to upload: all files under
        outputPaths (e.g. report) plus localModelRunParameters.json.
        Excludes inputPaths (downloaded parquet) so we do not upload inputs.
        :param local_model_run_parameters_path: optional path to localModelRunParameters.json
        :return: dict {file_name_wo_ext: file_path}
        """
        out = {}
        output_paths = self.local_directories.get("outputPaths") or {}
        for _key, dir_path in output_paths.items():
            if not dir_path or not os.path.isdir(dir_path):
                continue
            dir_abs = os.path.abspath(dir_path).replace("\\", "/")
            all_paths = gg(f'{dir_abs}{os.sep}**', recursive=True)
            for path in all_paths:
                if os.path.isfile(path):
                    name = os.path.splitext(os.path.basename(path))[0]
                    out[name] = os.path.abspath(path)
        if local_model_run_parameters_path and os.path.isfile(local_model_run_parameters_path):
            out["localModelRunParameters"] = os.path.abspath(local_model_run_parameters_path)
        return out

    def getModelRunParameters(self, s3_json_key, credentials) :
        model_run_parameters_path = f'{self.local_temp_directory}/{os.path.basename(s3_json_key)}'
        if self.local_mode :
            file = self._safeCopyFile(s3_json_key, model_run_parameters_path, raise_on_error=True)
        else :
            file = self._downloadFile(s3_json_key, model_run_parameters_path, raise_on_error=True)
        with open(model_run_parameters_path, 'r') as f :
            model_run_parameters_json = json.load(f)
        self.logger.debug(f'Contents of {os.path.basename(s3_json_key)}:\n{model_run_parameters_json}')
        return ModelRunParameters(model_run_parameters_json, file, credentials)

    # Only bucket-root "output/" with path: output/instrumentResult/analysisidentifier={id}/scenarioidentifier=Summary/
    OUTPUT_ROOT_BASE = "output"
    SCENARIO_SUMMARY_SEGMENT = "scenarioidentifier=Summary"
    # analysisDetails per analysis: export/analysisidentifier={id}/analysisDetails.json (same bucket root)
    EXPORT_BASE = "export"
    # Macro variables: under input root (not output/export), Baseline scenario only (sample/scenario.csv: BASE)
    INPUT_ROOT_BASE = "input"
    MACRO_SCENARIO_BASELINE = "BASE"

    ANALYSIS_METADATA_FILENAME = "analysis_metadata.json"

    @staticmethod
    def normalize_analyses_to_settings(settings):
        """
        Single structure: if settings has 'analyses' (array of { analysisId, quarterLabel?, tags? }).
        Order = chronological (oldest first). Optional tags (e.g. "current", "prior", "priorYear") let
        this report and others pick which analyses to use; we derive analysisIds, analysisRoles, quarterLabels.
        Backward compat: if "role" is present instead of "tags", treat it as a single-element tag.
        """
        analyses = settings.get("analyses")
        if not isinstance(analyses, list) or len(analyses) == 0:
            return
        analysis_ids = []
        current_id = prior_id = prior_year_id = None
        quarter_labels = {}
        for a in analyses:
            if isinstance(a, dict) and "analysisId" in a:
                aid = str(a["analysisId"])
                analysis_ids.append(aid)
                tags = a.get("tags")
                if not isinstance(tags, list) and a.get("role") is not None:
                    tags = [str(a.get("role")).strip()] if a.get("role") else []
                if isinstance(tags, list):
                    tags = [str(t).strip() for t in tags if t]
                    if "current" in tags:
                        current_id = aid
                    if "prior" in tags:
                        prior_id = aid
                    if "priorYear" in tags:
                        prior_year_id = aid
                if a.get("quarterLabel"):
                    quarter_labels[aid] = str(a["quarterLabel"]).strip()
            elif isinstance(a, dict) and "analysisId" not in a:
                continue
            else:
                aid = str(a)
                analysis_ids.append(aid)
        if not analysis_ids:
            return
        settings["analysisIds"] = analysis_ids
        settings["analysisRoles"] = {
            "current": current_id,
            "prior": prior_id,
            "priorYear": prior_year_id,
            "quarters": analysis_ids,
        }
        if quarter_labels:
            settings["quarterLabels"] = quarter_labels

    def _load_analysis_metadata_from_input(self):
        """
        If settings.analysisRoles is not already set, look for analysis_metadata.json under
        the execution input directory (optional payload). If found, load it and set
        settings['analysisRoles'] so the report builder can use current/prior/priorYear/quarters.
        """
        if self.model_run_parameters.settings.get("analysisRoles") is not None:
            print("[getSourceInputFiles] analysis metadata: already set in settings (skipping file load)")
            return
        input_dir = os.path.join(self.local_temp_directory, "input")
        if not os.path.isdir(input_dir):
            print("[getSourceInputFiles] analysis metadata: no input dir (optional; using callback analysisIds + date-inferred roles)")
            return
        found = None
        for root, _dirs, files in os.walk(input_dir):
            if self.ANALYSIS_METADATA_FILENAME in files:
                found = os.path.join(root, self.ANALYSIS_METADATA_FILENAME)
                break
        if not found:
            print("[getSourceInputFiles] analysis metadata: {} not found under input (optional)".format(self.ANALYSIS_METADATA_FILENAME))
            return
        try:
            with open(found, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, JSONDecodeError) as e:
            self.logger.warning("Failed to load %s: %s", found, e)
            return
        # One structure: "analyses" array -> normalize to analysisIds, analysisRoles, quarterLabels.
        # Legacy: "current", "prior", "priorYear", "quarters" -> set analysisRoles (and quarterLabels if quarters are objects).
        if not isinstance(data, dict):
            return
        if "analyses" in data and isinstance(data["analyses"], list):
            self.model_run_parameters.settings["analyses"] = data["analyses"]
            IOSession.normalize_analyses_to_settings(self.model_run_parameters.settings)
            self.logger.info("Loaded analyses from %s: %s analyses -> analysisIds, analysisRoles", found, len(data["analyses"]))
            print("[getSourceInputFiles] Loaded analysis metadata from {} (analyses -> analysisIds, roles)".format(os.path.basename(found)))
            return
        roles = dict(data)
        quarters_raw = roles.get("quarters")
        quarter_labels = {}
        if isinstance(quarters_raw, list):
            quarter_ids = []
            for item in quarters_raw:
                if isinstance(item, dict) and "analysisId" in item:
                    aid = str(item["analysisId"])
                    quarter_ids.append(aid)
                    if item.get("quarterLabel"):
                        quarter_labels[aid] = str(item["quarterLabel"])
                else:
                    quarter_ids.append(str(item))
            roles["quarters"] = quarter_ids
            if quarter_labels:
                self.model_run_parameters.settings["quarterLabels"] = quarter_labels
        self.model_run_parameters.settings["analysisRoles"] = roles
        self.logger.info("Loaded analysis roles from %s: current=%s, prior=%s", found, roles.get("current"), roles.get("prior"))
        print("[getSourceInputFiles] Loaded analysis metadata from {}".format(os.path.basename(found)))

    def _get_macro_scenario_date_from_analysis_details(self, report_dir, analysis_id):
        """
        Load analysisDetails_{id}.json from report_dir and return asOfDate for the BASE scenario
        (scenarios[].name == "BASE" -> asOfDate). Returns YYYY-MM-DD string or None.
        """
        if not report_dir or not analysis_id:
            return None
        path = os.path.join(report_dir, "analysisDetails_{}.json".format(analysis_id))
        if not os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, JSONDecodeError):
            return None
        for s in (data.get("scenarios") or []):
            if str(s.get("name")).strip().upper() == "BASE":
                d = s.get("asOfDate")
                if d:
                    return self._normalize_date_for_path(d)
        return None

    def _get_reporting_date_from_analysis_details(self, report_dir, analysis_id):
        """Return reportingDate from analysisDetails as YYYY-MM-DD or None."""
        if not report_dir or not analysis_id:
            return None
        path = os.path.join(report_dir, "analysisDetails_{}.json".format(analysis_id))
        if not os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, JSONDecodeError):
            return None
        d = data.get("reportingDate")
        return self._normalize_date_for_path(d) if d else None

    def _normalize_date_for_path(self, value):
        """Return date as YYYY-MM-DD for S3 path segment (asofdate=YYYY-MM-DD)."""
        if not value:
            return None
        try:
            dt = pd.to_datetime(value)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None

    def getSourceInputFiles(self) :
        print("[getSourceInputFiles] callBack={}".format(self.model_run_parameters.callBack))
        if self.model_run_parameters.callBack is True :
            source_input_directory = self.local_directories.get('inputPaths')
            input_files = {}
            analysis_ids = self.model_run_parameters.settings.get('analysisIds', [])
            n_analyses = len(analysis_ids or [])
            print("[getSourceInputFiles] analysisIds count={}".format(n_analyses))

            base = self.OUTPUT_ROOT_BASE
            scenario = self.SCENARIO_SUMMARY_SEGMENT
            instrument_result_paths = source_input_directory.get("instrumentResult") or []
            for idx, aid in enumerate(analysis_ids or []):
                if idx >= len(instrument_result_paths):
                    break
                local_dir = instrument_result_paths[idx]
                os.makedirs(local_dir, exist_ok=True)
                prefix = "{}/instrumentResult/analysisidentifier={}/{}/".format(base, aid, scenario)
                keys = self._get_s3_object_keys(prefix)
                parquet_keys = [k for k in keys if k.endswith(".parquet")]
                for s3_key in parquet_keys:
                    local_path = os.path.join(local_dir, os.path.basename(s3_key))
                    if self.local_mode:
                        self._safeCopyFile(s3_key, local_path)
                    else:
                        self._downloadFile(s3_key, local_path)
                    input_files.update({os.path.splitext(os.path.basename(s3_key))[0]: local_path})

            # Download instrumentReporting: output/instrumentReporting/analysisidentifier={id}/ (parquet files directly under, no extra partition)
            instrument_reporting_paths = source_input_directory.get("instrumentReporting") or []
            for idx, aid in enumerate(analysis_ids or []):
                if idx >= len(instrument_reporting_paths):
                    break
                local_dir = instrument_reporting_paths[idx]
                os.makedirs(local_dir, exist_ok=True)
                prefix = "{}/instrumentReporting/analysisidentifier={}/".format(base, aid)
                keys = self._get_s3_object_keys(prefix)
                parquet_keys = [k for k in keys if k.endswith(".parquet")]
                for s3_key in parquet_keys:
                    local_path = os.path.join(local_dir, os.path.basename(s3_key))
                    if self.local_mode:
                        self._safeCopyFile(s3_key, local_path)
                    else:
                        self._downloadFile(s3_key, local_path)

            # Download instrumentReference: output/instrumentReference/analysisidentifier={id}/ (partitioned by portfolioidentifier=.../; get all parquet from all subfolders)
            instrument_reference_paths = source_input_directory.get("instrumentReference") or []
            for idx, aid in enumerate(analysis_ids or []):
                if idx >= len(instrument_reference_paths):
                    break
                local_dir = instrument_reference_paths[idx]
                os.makedirs(local_dir, exist_ok=True)
                prefix = "{}/instrumentReference/analysisidentifier={}/".format(base, aid)
                keys = self._get_s3_object_keys(prefix)
                parquet_keys = [k for k in keys if k.endswith(".parquet")]
                for s3_key in parquet_keys:
                    # Preserve subpath under analysisidentifier={id}/ to avoid overwriting (e.g. portfolioidentifier=CRE/part-*.parquet)
                    rel = s3_key[len(prefix):] if s3_key.startswith(prefix) else os.path.basename(s3_key)
                    local_path = os.path.join(local_dir, rel.replace("/", os.sep))
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    if self.local_mode:
                        self._safeCopyFile(s3_key, local_path)
                    else:
                        self._downloadFile(s3_key, local_path)

            report_dir = self.local_directories.get("outputPaths", {}).get("report")
            if report_dir:
                os.makedirs(report_dir, exist_ok=True)
                for aid in (analysis_ids or []):
                    s3_key = "{}/analysisidentifier={}/analysisDetails.json".format(self.EXPORT_BASE, aid)
                    local_path = os.path.join(report_dir, "analysisDetails_{}.json".format(aid))
                    if self.local_mode:
                        self._safeCopyFile(s3_key, local_path)
                    else:
                        self._downloadFile(s3_key, local_path)

            # Download macroEconomicVariableInput from input root: path uses asofdate from analysisDetails (main analysis) and scenarioidentifier=BASE
            macro_paths = source_input_directory.get("macroEconomicVariableInput") or []
            main_analysis_id = (self.model_run_parameters.settings.get("analysisRoles") or {}).get("current")
            if main_analysis_id is None and analysis_ids:
                main_analysis_id = analysis_ids[0]
            scenario_asof_date = self._get_macro_scenario_date_from_analysis_details(report_dir, main_analysis_id)
            if not scenario_asof_date:
                print("[getSourceInputFiles] macro: no asOfDate from analysisDetails (will try default path)")
            for idx, aid in enumerate(analysis_ids or []):
                if idx >= len(macro_paths):
                    break
                local_dir = macro_paths[idx]
                os.makedirs(local_dir, exist_ok=True)
                if not self.local_mode:
                    # Path: input/macroeconomicVariableInput/asofdate=YYYY-MM-DD/scenarioidentifier=BASE/
                    cat_folder = "macroeconomicVariableInput"
                    try_date_used = scenario_asof_date
                    if not try_date_used:
                        try_date_used = self._get_reporting_date_from_analysis_details(report_dir, main_analysis_id or aid)
                    parquet_keys = []
                    for try_date in (try_date_used, None):
                        if try_date:
                            prefix = "{}/{}/asofdate={}/scenarioidentifier={}/".format(
                                self.INPUT_ROOT_BASE, cat_folder, try_date, self.MACRO_SCENARIO_BASELINE)
                        else:
                            # Fallback: no asofdate segment (legacy)
                            prefix = "{}/{}/analysisidentifier={}/scenarioidentifier={}/".format(
                                self.INPUT_ROOT_BASE, "macroEconomicVariableInput", aid, self.MACRO_SCENARIO_BASELINE)
                        keys = self._get_s3_object_keys(prefix)
                        parquet_keys = [k for k in keys if k.endswith(".parquet")]
                        if parquet_keys:
                            for s3_key in parquet_keys:
                                local_path = os.path.join(local_dir, os.path.basename(s3_key))
                                self._downloadFile(s3_key, local_path)
                            break
                    if not parquet_keys:
                        print("[getSourceInputFiles] macro: no parquet at input/.../asofdate=.../scenarioidentifier=BASE/")
                else:
                    # Local mode: copy from test folder if present (same prefix structure under test input path)
                    pass

            input_directory = self.local_directories.get('inputPath')
            # for dirs in input_directory['custInputs']:
            #     os.makedirs(dirs, exist_ok=True)
            # os.makedirs(input_directory, exist_ok=True)
            for name, paths in input_directory.items() :
                for files in paths :
                    # print(files)
                    loc_file_path = files
                    rem_file_path = loc_file_path.replace(f'{self.local_temp_directory}/input', self.input_path)
                    if self.local_mode :
                        file = self._safeCopyFile(rem_file_path, loc_file_path)
                    else :
                        file = self._downloadFile(rem_file_path, loc_file_path)

            self._load_analysis_metadata_from_input()
            print("[getSourceInputFiles] done: result, reporting, ref, analysisDetails (and macro if present) for {} analyses".format(n_analyses))

        else :
            print("[getSourceInputFiles] non-callback branch: using inputPath.custInputs")
            source_input_directory = self.local_directories.get('inputPath')
            input_files = {}
            # S3 / create_io_directories builds inputPath as {'custInputs': [list of full local file paths]}
            if 'custInputs' in source_input_directory:
                paths = source_input_directory['custInputs']
                print(f"[getSourceInputFiles] Downloading {len(paths)} input file(s) from S3")
                for name, path_list in source_input_directory.items():
                    for loc_file_path in path_list:
                        os.makedirs(os.path.dirname(loc_file_path), exist_ok=True)
                        rem_file_path = loc_file_path.replace(f'{self.local_temp_directory}/input', self.input_path)
                        if self.local_mode:
                            file = self._safeCopyFile(rem_file_path, loc_file_path)
                    else:
                        file = self._downloadFile(rem_file_path, loc_file_path)
                    input_files.update(file)
            else:
                # Legacy: inputPath keyed by file name (e.g. 'instrumentReference.csv' -> directory path)
                for dirs in source_input_directory:
                    os.makedirs(dirs, exist_ok=True)
                file_names = [f'{fn}.csv' for fn in {**self.model_run_parameters.input_data, **self.model_run_parameters.supporting_data}]
                for file_name in file_names:
                    local_file_path = f'{source_input_directory[file_name]}/{file_name}'
                    remote_file_path = f'{self.input_path}/{file_name}'
                    if self.local_mode:
                        file = self._safeCopyFile(remote_file_path, local_file_path)
                    else:
                        file = self._downloadFile(remote_file_path, local_file_path)
                    input_files.update(file)
            self._load_analysis_metadata_from_input()
        print("[getSourceInputFiles] done")
        return input_files

    def uploadFiles(self, files, scenario_name=None) :
        n = len(files)
        keys = list(files.keys())
        print("[uploadFiles] uploading {} file(s) to S3{}".format(n, ": " + ", ".join(keys) if n <= 10 else ""))
        for file, file_path in files.items() :
            ext = os.path.splitext(file_path)[1]
            out_path = self.model_run_parameters.output_s3_paths.get(file)
            if self.local_mode :
                if out_path and scenario_name :
                    dest_path = f'{self.test_folder_output}/{file}/scenarioPartition={scenario_name}/data{ext}'
                elif out_path :
                    dest_path = f'{self.test_folder_output}/{file}/data{ext}'
                else :
                    dest_path = f'{self.test_folder_output}/log/{os.path.basename(file_path)}'
                self._safeCopyFile(file_path, dest_path)
            else :
                if os.name == 'nt':
                    root_path = '/'.join((file_path.split('\\')[:-1]))
                else:
                    root_path = '/'.join((file_path.split('/')[:-1]))
                upload_path = root_path.replace(self.local_temp_directory, self.model_run_parameters.output_s3_path)
                s3_key = f'{upload_path}/{os.path.basename(file_path)}'
                self._uploadFile(file_path, s3_key)
