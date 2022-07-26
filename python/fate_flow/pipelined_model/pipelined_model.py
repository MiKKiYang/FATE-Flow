#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import base64
import hashlib
import json
import os
import shutil
import typing
from functools import wraps

from google.protobuf import json_format
from ruamel import yaml

from fate_arch.common.base_utils import json_dumps, json_loads

from fate_flow.component_env_utils import provider_utils
from fate_flow.db.runtime_config import RuntimeConfig
from fate_flow.model import Locker, parse_proto_object, serialize_buffer_object
from fate_flow.pipelined_model.pipelined_component import PipelinedComponent
from fate_flow.protobuf.python.pipeline_pb2 import Pipeline
from fate_flow.settings import TEMP_DIRECTORY, stat_logger
from fate_flow.utils.job_utils import job_pipeline_component_name, job_pipeline_component_module_name
from fate_flow.utils.base_utils import get_fate_flow_directory, get_fate_flow_python_directory


def local_cache_required(method):
    @wraps(method)
    def magic(self, *args, **kwargs):
        if not self.exists():
            raise FileNotFoundError(f'Can not found {self.model_id} {self.model_version} model local cache')
        return method(self, *args, **kwargs)
    return magic


class PipelinedModel(Locker):
    def __init__(self, model_id, model_version):
        """
        Support operations on FATE PipelinedModels
        :param model_id: the model id stored at the local party.
        :param model_version: the model version.
        """
        os.makedirs(TEMP_DIRECTORY, exist_ok=True)

        self.role, self.party_id, self._model_id = model_id.split('#', 2)
        self.party_model_id = self.model_id = model_id
        self.model_version = model_version
        self.model_path = get_fate_flow_directory("model_local_cache", model_id, model_version)
        self.define_proto_path = os.path.join(self.model_path, "define", "proto")
        self.define_proto_generated_path = os.path.join(self.model_path, "define", "proto_generated_python")
        self.define_meta_path = os.path.join(self.model_path, "define", "define_meta.yaml")
        self.variables_index_path = os.path.join(self.model_path, "variables", "index")
        self.variables_data_path = os.path.join(self.model_path, "variables", "data")
        self.run_parameters_path = os.path.join(self.model_path, "run_parameters")
        self.default_archive_format = "zip"
        self.pipeline_model_name = "Pipeline"
        self.pipeline_model_alias = "pipeline"

        self.pipelined_component = PipelinedComponent(role=self.role, party_id=self.party_id, model_id=self._model_id, model_version=self.model_version)

        super().__init__(self.model_path)

    def create_pipelined_model(self):
        if self.exists():
            raise FileExistsError("Model creation failed because it has already been created, model cache path is {}".
                                  format(self.model_path))
        os.makedirs(self.model_path)

        with self.lock:
            for path in [self.variables_index_path, self.variables_data_path]:
                os.makedirs(path)
            shutil.copytree(get_fate_flow_python_directory("fate_flow", "protobuf", "proto"), self.define_proto_path)
            shutil.copytree(get_fate_flow_python_directory("fate_flow", "protobuf", "python"), self.define_proto_generated_path)

    def save_pipeline_model(self, pipeline_buffer_object):
        model_buffers = {self.pipeline_model_name: (type(pipeline_buffer_object).__name__, pipeline_buffer_object.SerializeToString(), json_format.MessageToDict(pipeline_buffer_object, including_default_value_fields=True))}
        self.save_component_model(component_name=job_pipeline_component_name(),
                                  component_module_name=job_pipeline_component_module_name(),
                                  model_alias=self.pipeline_model_alias,
                                  model_buffers=model_buffers)

        with self.lock, open(self.define_meta_path, 'w', encoding="utf-8") as f:
            define_meta = self.pipelined_component.read_define_meta()
            yaml.dump(define_meta, f, Dumper=yaml.RoundTripDumper)

    def save_component_model(self, component_name, component_module_name, model_alias, model_buffers, user_specified_run_parameters=None):
        component_model = self.create_component_model(component_name=component_name,
                                                      component_module_name=component_module_name,
                                                      model_alias=model_alias,
                                                      model_buffers=model_buffers,
                                                      user_specified_run_parameters=user_specified_run_parameters)
        self.write_component_model(component_model)

    def create_component_model(self, component_name, component_module_name, model_alias, model_buffers: typing.Dict[str, typing.Tuple[str, bytes, dict]], user_specified_run_parameters: dict = None):
        model_proto_index = {}
        component_model = {"buffer": {}}
        component_model_storage_path = os.path.join(self.variables_data_path, component_name, model_alias)
        for model_name, (proto_index, object_serialized, object_json) in model_buffers.items():
            storage_path = os.path.join(component_model_storage_path, model_name)
            component_model["buffer"][storage_path.replace(get_fate_flow_directory(), "")] = (base64.b64encode(object_serialized).decode(), object_json)
            model_proto_index[model_name] = proto_index  # index of model name and proto buffer class name
            stat_logger.info("save {} {} {} buffer".format(component_name, model_alias, model_name))
        component_model["component_name"] = component_name
        component_model["component_module_name"] = component_module_name
        component_model["model_alias"] = model_alias
        component_model["model_proto_index"] = model_proto_index
        component_model["run_parameters"] = user_specified_run_parameters
        return component_model

    def write_component_model(self, component_model):
        for storage_path, (object_serialized_encoded, object_json) in component_model.get("buffer").items():
            storage_path = get_fate_flow_directory() + storage_path
            os.makedirs(os.path.dirname(storage_path), exist_ok=True)
            with self.lock, open(storage_path, "wb") as fw:
                fw.write(base64.b64decode(object_serialized_encoded.encode()))
            with self.lock, open(f"{storage_path}.json", "w", encoding="utf8") as fw:
                fw.write(json_dumps(object_json))
        run_parameters = component_model.get("run_parameters") or {}
        p = self.component_run_parameters_path(component_model["component_name"])
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with self.lock, open(p, "w", encoding="utf8") as fw:
            fw.write(json_dumps(run_parameters))
        self.update_component_meta(component_name=component_model["component_name"],
                                   component_module_name=component_model["component_module_name"],
                                   model_alias=component_model["model_alias"],
                                   model_proto_index=component_model["model_proto_index"])
        stat_logger.info("save {} {} successfully".format(component_model["component_name"],
                                                          component_model["model_alias"]))

    @local_cache_required
    def _read_component_model(self, component_name, model_alias):
        component_model_storage_path = os.path.join(self.variables_data_path, component_name, model_alias)
        model_proto_index = self.get_model_proto_index(component_name=component_name, model_alias=model_alias)

        model_buffers = {}
        for model_name, buffer_name in model_proto_index.items():
            storage_path = os.path.join(component_model_storage_path, model_name)

            with open(storage_path, "rb") as f:
                buffer_object_serialized_string = f.read()

            try:
                with open(f"{storage_path}.json", encoding="utf8") as f:
                    buffer_object_json_format = json_loads(f.read())
            except FileNotFoundError:
                buffer_object_json_format = ""
                # TODO: should be running in worker
                """
                buffer_object_json_format = json_format.MessageToDict(
                    parse_proto_object(buffer_name, buffer_object_serialized_string),
                    including_default_value_fields=True
                )
                with self.lock, open(f"{storage_path}.json", "w", encoding="utf8") as f:
                    f.write(json_dumps(buffer_object_json_format))
                """

            model_buffers[model_name] = (
                buffer_name,
                buffer_object_serialized_string,
                buffer_object_json_format,
            )

        return model_buffers

    # TODO: use different functions instead of passing arguments
    def read_component_model(self, component_name, model_alias, parse=True, output_json=False):
        _model_buffers = self._read_component_model(component_name, model_alias)

        model_buffers = {}
        for model_name, (
            buffer_name,
            buffer_object_serialized_string,
            buffer_object_json_format,
        ) in _model_buffers.items():
            if output_json:
                model_buffers[model_name] = buffer_object_json_format
            elif parse:
                model_buffers[model_name] = parse_proto_object(buffer_name, buffer_object_serialized_string)
            else:
                model_buffers[model_name] = [
                    buffer_name,
                    base64.b64encode(buffer_object_serialized_string).decode("ascii"),
                ]

        return model_buffers

    # TODO: integration with read_component_model
    def read_pipeline_model(self, parse=True):
        component_name = job_pipeline_component_name()
        model_alias = self.pipeline_model_alias
        component_model_storage_path = os.path.join(self.variables_data_path, component_name, model_alias)
        model_proto_index = self.get_model_proto_index(component_name=component_name,
                                                       model_alias=model_alias)
        model_buffers = {}
        for model_name, buffer_name in model_proto_index.items():
            storage_path = os.path.join(component_model_storage_path, model_name)
            with open(storage_path, "rb") as fr:
                buffer_object_serialized_string = fr.read()
                if parse:
                    model_buffers[model_name] = parse_proto_object(buffer_name=buffer_name,
                                                                   serialized_string=buffer_object_serialized_string,
                                                                   buffer_class=Pipeline)
                else:
                    model_buffers[model_name] = [buffer_name, base64.b64encode(buffer_object_serialized_string).decode()]
        return model_buffers[self.pipeline_model_name]

    def read_model_run_parameters(self):
        if not os.path.exists(self.run_parameters_path):
            return {}
        components_run_parameters = {}
        for component_name in os.listdir(self.run_parameters_path):
            p = self.component_run_parameters_path(component_name)
            with open(p, encoding="utf8") as fr:
                components_run_parameters[component_name] = json_loads(fr.read())
        return components_run_parameters

    @local_cache_required
    def collect_models(self, in_bytes=False, b64encode=True):
        define_meta = self.pipelined_component.read_define_meta()
        model_buffers = {}

        for component_name in define_meta.get("model_proto", {}).keys():
            for model_alias, model_proto_index in define_meta["model_proto"][component_name].items():
                component_model_storage_path = os.path.join(self.variables_data_path, component_name, model_alias)
                for model_name, buffer_name in model_proto_index.items():
                    storage_path = os.path.join(component_model_storage_path, model_name)
                    with open(storage_path, "rb") as fr:
                        serialized_string = fr.read()
                    if not in_bytes:
                        model_buffers[model_name] = parse_proto_object(buffer_name, serialized_string)
                    else:
                        if b64encode:
                            serialized_string = base64.b64encode(serialized_string).decode()
                        model_buffers[f"{component_name}.{model_alias}:{model_name}"] = serialized_string

        return model_buffers

    @staticmethod
    def get_model_migrate_tool():
        return provider_utils.get_provider_class_object(RuntimeConfig.COMPONENT_PROVIDER, "model_migrate", True)

    @staticmethod
    def get_homo_model_convert_tool():
        return provider_utils.get_provider_class_object(RuntimeConfig.COMPONENT_PROVIDER, "homo_model_convert", True)

    def exists(self):
        return os.path.isdir(self.model_path) and set(os.listdir(self.model_path)) - {'.lock'}

    def save_protobuf(self, buffer_object, filepath):
        serialized_string = serialize_buffer_object(buffer_object)
        with self.lock, open(filepath, "wb") as fw:
            fw.write(serialized_string)
        return filepath

    def save_pipeline(self, buffer_object):
        return self.save_protobuf(buffer_object, os.path.join(self.model_path, "pipeline.pb"))

    @local_cache_required
    def packaging_model(self):
        with self.lock:
            # self.archive_model_file_path
            shutil.make_archive(base_name=self.archive_model_base_path, format=self.default_archive_format, root_dir=self.model_path)

            with open(self.archive_model_file_path, 'rb') as f:
                hash_ = hashlib.sha256(f.read()).hexdigest()

        stat_logger.info(f'Make model {self.model_id} {self.model_version} archive successfully. path: {self.archive_model_file_path} hash: {hash_}')
        return hash_

    def unpack_model(self, archive_file_path: str, force_update: bool = False, hash_: str = None):
        os.makedirs(self.model_path)

        with self.lock:
            if self.exists() and not force_update:
                raise FileExistsError(f'Model {self.model_id} {self.model_version} local cache already existed.')

            if hash_ is not None:
                with open(archive_file_path, 'rb') as f:
                    sha256 = hashlib.sha256(f.read()).hexdigest()

                if hash_ != sha256:
                    raise ValueError(f'Model archive hash mismatch. path: {archive_file_path} expected: {hash_} actual: {sha256}')

            shutil.unpack_archive(archive_file_path, self.model_path)

        stat_logger.info(f'Unpack model {self.model_id} {self.model_version} archive successfully. path: {self.model_path}')

    @local_cache_required
    def update_component_meta(self, component_name, component_module_name, model_alias, model_proto_index):
        """
        update meta info yaml
        :param component_name:
        :param component_module_name:
        :param model_alias:
        :param model_proto_index:
        :return:
        """
        return self.pipelined_component.write_define_meta(component_name, component_module_name, model_alias, model_proto_index)

    def get_component_define(self, component_name=None):
        component_define = self.pipelined_component.read_define_meta()['component_define']
        if component_name is None:
            return component_define
        return component_define.get(component_name, {})

    def get_model_proto_index(self, component_name=None, model_alias=None):
        model_proto = self.pipelined_component.read_define_meta()['model_proto']
        if component_name is None:
            return model_proto
        model_proto = model_proto.get(component_name, {})
        if model_alias is None:
            return model_proto
        return model_proto.get(model_alias, {})

    def get_model_alias(self, component_name):
        model_proto_index = self.get_model_proto_index(component_name)

        if len(model_proto_index.keys()) != 1:
            raise KeyError('Failed to detect "model_alias", please specify it manually.')

        return list(model_proto_index.keys())[0]

    @property
    def archive_model_base_path(self):
        return os.path.join(TEMP_DIRECTORY, "{}_{}".format(self.model_id, self.model_version))

    @property
    def archive_model_file_path(self):
        return "{}.{}".format(self.archive_model_base_path, self.default_archive_format)

    def calculate_model_file_size(self):
        size = 0
        for root, dirs, files in os.walk(self.model_path):
            size += sum([os.path.getsize(os.path.join(root, name)) for name in files])
        return round(size/1024)

    def component_run_parameters_path(self, component_name):
        return os.path.join(self.run_parameters_path, component_name, "run_parameters.json")

    def reload_component_model(self, model_id, model_version, component_list):
        for component_name in component_list:
            target_path = os.path.join(self.variables_data_path, component_name)
            source_pipeline_model = PipelinedModel(model_id, model_version)
            source_path = os.path.join(source_pipeline_model.variables_data_path, component_name)
            if not os.path.exists(source_path):
                continue
            shutil.copytree(source_path, target_path)

            # update meta
            component_model_proto = source_pipeline_model.get_model_proto_index(component_name)
            component_define = source_pipeline_model.get_component_define(component_name)
            for model_alias, model_proto_index in component_model_proto.items():
                self.update_component_meta(component_name, component_define.get("module_name"), model_alias, model_proto_index)

    def gen_model_import_config(self):
        config = {
            'role': self.role,
            'party_id': int(self.party_id),
            'model_id': self._model_id,
            'model_version': self.model_version,
            'file': self.archive_model_file_path,
        }
        with self.lock, open(os.path.join(self.model_path, 'import_model.json'), 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)
