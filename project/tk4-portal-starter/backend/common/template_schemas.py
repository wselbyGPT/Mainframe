from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

JOB_NAME_PATTERN = re.compile(r'^[A-Z][A-Z0-9#$@]{0,7}$')


class TemplateSchemaError(ValueError):
    """Structured validation error for template schemas."""

    def __init__(self, code: str, message: str, errors: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.errors = errors or []

    def to_dict(self) -> dict[str, Any]:
        return {
            'code': self.code,
            'message': self.message,
            'errors': self.errors,
        }


class UnknownTemplateError(TemplateSchemaError):
    """Raised when a template_id does not exist."""


TEMPLATE_PACKS: list[dict[str, Any]] = [
    {
        'operations_pack_id': 'ops-core',
        'version': '1.0.0',
        'description': 'Core z/OS operations templates for common utility and catalog workflows.',
        'compatibility': {
            'target_images': ['TK4-'],
            'target_profiles': ['starter', 'full'],
        },
        'params': {
            'job_name': {
                'type': 'string',
                'required': False,
                'default': 'WEBJOB',
                'format': 'jcl_job_name',
                'help': 'JCL job name (1-8 chars, starts with A-Z, then A-Z0-9#$@).',
                'examples': ['HELLO1', 'MYJOB99'],
            },
        },
        'templates': [
            {
                'template_id': 'hello-world',
                'description': 'Print a literal message to SYSOUT via IEBGENER.',
                'compatibility': {
                    'target_images': ['TK4-'],
                    'target_profiles': ['starter', 'full'],
                },
                'params': {
                    'job_name': {'default': 'HELLO1'},
                    'message': {
                        'type': 'string',
                        'required': False,
                        'default': 'HELLO FROM WEB PORTAL',
                        'help': 'Message line written to SYSUT1 in-stream data.',
                        'examples': ['HELLO FROM THE WEB PORTAL'],
                    },
                },
            },
            {
                'template_id': 'idcams-listcat',
                'description': 'Run IDCAMS LISTCAT for a catalog or level filter.',
                'compatibility': {
                    'target_images': ['TK4-'],
                    'target_profiles': ['starter', 'full'],
                },
                'params': {
                    'job_name': {'default': 'LISTCAT', 'examples': ['LISTCAT']},
                    'level': {
                        'type': 'string',
                        'required': True,
                        'help': 'LISTCAT LEVEL operand (for example: SYS1 or USER.TEST).',
                        'examples': ['SYS1', 'USER.TEST'],
                    },
                },
            },
            {
                'template_id': 'iebgener-copy',
                'description': 'Copy one sequential dataset to another via IEBGENER.',
                'compatibility': {
                    'target_images': ['TK4-'],
                    'target_profiles': ['full'],
                },
                'params': {
                    'job_name': {'default': 'IEBGEN', 'examples': ['COPYJOB']},
                    'input_dataset': {
                        'type': 'string',
                        'required': True,
                        'help': 'Input DSN to read (quoted in JCL).',
                        'examples': ['SYS1.PROCLIB'],
                    },
                    'output_dataset': {
                        'type': 'string',
                        'required': True,
                        'help': 'Output DSN to write (quoted in JCL).',
                        'examples': ['IBMUSER.PROCLIB'],
                    },
                },
            },
            {
                'template_id': 'sort-basic',
                'description': 'Run SORT with a simple key on fixed records.',
                'compatibility': {
                    'target_images': ['TK4-'],
                    'target_profiles': ['full'],
                },
                'params': {
                    'job_name': {'default': 'SORTJOB', 'examples': ['SORTJOB']},
                    'input_dataset': {
                        'type': 'string',
                        'required': True,
                        'help': 'Input DSN to sort (quoted in JCL).',
                        'examples': ['IBMUSER.INPUT'],
                    },
                    'output_dataset': {
                        'type': 'string',
                        'required': True,
                        'help': 'Output DSN for sorted records (quoted in JCL).',
                        'examples': ['IBMUSER.OUTPUT'],
                    },
                    'sort_fields': {
                        'type': 'string',
                        'required': False,
                        'default': '1,10,CH,A',
                        'help': 'DFSORT SORT FIELDS expression.',
                        'examples': ['1,10,CH,A', '1,8,CH,D'],
                    },
                },
            },
            {
                'template_id': 'lattice-crypto-demo',
                'description': 'Emit a lattice cryptography runbook stub to SYSOUT for mainframe operator workflows.',
                'compatibility': {
                    'target_images': ['TK4-'],
                    'target_profiles': ['starter', 'full'],
                },
                'params': {
                    'job_name': {'default': 'LATTICE', 'examples': ['LATTICE', 'PQCDEMO1']},
                    'algorithm': {
                        'type': 'string',
                        'required': False,
                        'default': 'CRYSTALS-KYBER',
                        'help': 'Lattice KEM or signature algorithm label for run documentation.',
                        'examples': ['CRYSTALS-KYBER', 'CRYSTALS-DILITHIUM', 'FALCON'],
                    },
                    'security_level': {
                        'type': 'string',
                        'required': False,
                        'default': 'LEVEL1',
                        'help': 'Target parameter set/security tier label.',
                        'examples': ['LEVEL1', 'LEVEL3', 'LEVEL5'],
                    },
                    'key_dataset': {
                        'type': 'string',
                        'required': True,
                        'help': 'DSN placeholder for where generated key material should be managed.',
                        'examples': ['IBMUSER.PQC.KEYS'],
                    },
                    'notes': {
                        'type': 'string',
                        'required': False,
                        'default': 'PILOT - VALIDATE DATASET ACLS AND ROTATION POLICY',
                        'help': 'Free-form operations note recorded in SYSOUT.',
                        'examples': ['PILOT IN LPAR2', 'ROTATE KEYS WEEKLY'],
                    },
                },
            },
        ],
    }
]


def _unknown_template_error(template_id: str) -> UnknownTemplateError:
    supported = sorted(_TEMPLATE_SCHEMAS)
    return UnknownTemplateError(
        code='unknown_template_id',
        message=f"Unknown template_id '{template_id}'",
        errors=[
            {
                'path': 'template_id',
                'reason': 'unsupported_value',
                'expected': {'one_of': supported},
                'actual': template_id,
            }
        ],
    )


def _normalize_string(value: str) -> str:
    return value.strip()


def _normalize_job_name(value: str) -> str:
    candidate = _normalize_string(value).upper()[:8]
    if not candidate:
        raise TemplateSchemaError(
            code='template_params_invalid',
            message='Template parameters failed validation',
            errors=[
                {
                    'path': 'params.job_name',
                    'reason': 'empty_value',
                    'expected': {'type': 'string', 'format': 'jcl_job_name'},
                }
            ],
        )
    if not JOB_NAME_PATTERN.fullmatch(candidate):
        raise TemplateSchemaError(
            code='template_params_invalid',
            message='Template parameters failed validation',
            errors=[
                {
                    'path': 'params.job_name',
                    'reason': 'invalid_format',
                    'expected': {'type': 'string', 'format': 'jcl_job_name', 'pattern': JOB_NAME_PATTERN.pattern},
                    'actual': candidate,
                }
            ],
        )
    return candidate


def validate_template_pack_structure(template_packs: list[dict[str, Any]]) -> None:
    errors: list[dict[str, Any]] = []
    known_template_ids: set[str] = set()

    for pack_index, pack in enumerate(template_packs):
        pack_path = f'packs[{pack_index}]'
        if not isinstance(pack.get('operations_pack_id'), str) or not pack['operations_pack_id'].strip():
            errors.append({'path': f'{pack_path}.operations_pack_id', 'reason': 'missing_or_invalid'})
        if not isinstance(pack.get('version'), str) or not pack['version'].strip():
            errors.append({'path': f'{pack_path}.version', 'reason': 'missing_or_invalid'})
        if not isinstance(pack.get('description'), str) or not pack['description'].strip():
            errors.append({'path': f'{pack_path}.description', 'reason': 'missing_or_invalid'})

        pack_params = pack.get('params', {})
        if not isinstance(pack_params, dict):
            errors.append({'path': f'{pack_path}.params', 'reason': 'invalid_type', 'expected': {'type': 'object'}})
            pack_params = {}
        pack_templates = pack.get('templates', [])
        if not isinstance(pack_templates, list):
            errors.append({'path': f'{pack_path}.templates', 'reason': 'invalid_type', 'expected': {'type': 'array'}})
            pack_templates = []

        for param_name, param_schema in pack_params.items():
            if not isinstance(param_schema, dict) or not isinstance(param_schema.get('type'), str):
                errors.append({'path': f'{pack_path}.params.{param_name}', 'reason': 'invalid_param_schema'})

        for template_index, template in enumerate(pack_templates):
            template_path = f'{pack_path}.templates[{template_index}]'
            template_id = template.get('template_id')
            if not isinstance(template_id, str) or not template_id.strip():
                errors.append({'path': f'{template_path}.template_id', 'reason': 'missing_or_invalid'})
                continue
            if template_id in known_template_ids:
                errors.append({'path': f'{template_path}.template_id', 'reason': 'duplicate_template_id', 'actual': template_id})
            known_template_ids.add(template_id)
            if not isinstance(template.get('params', {}), dict):
                errors.append({'path': f'{template_path}.params', 'reason': 'invalid_type', 'expected': {'type': 'object'}})
                continue
            for param_name, param_schema in template.get('params', {}).items():
                if not isinstance(param_schema, dict):
                    errors.append({'path': f'{template_path}.params.{param_name}', 'reason': 'invalid_param_schema'})
                elif 'type' not in param_schema and param_name not in pack_params:
                    errors.append({'path': f'{template_path}.params.{param_name}.type', 'reason': 'missing_required_field'})

    if errors:
        raise TemplateSchemaError(
            code='template_catalog_invalid',
            message='Template pack definitions failed validation',
            errors=errors,
        )


def _merge_param_schema(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    merged.update(override)
    return merged


def _flatten_template_packs(template_packs: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    validate_template_pack_structure(template_packs)
    flattened: dict[str, dict[str, Any]] = {}
    template_to_pack: dict[str, dict[str, Any]] = {}

    for pack in template_packs:
        pack_params = pack.get('params', {})
        pack_compat = deepcopy(pack.get('compatibility', {}))
        for template in pack.get('templates', []):
            template_id = template['template_id']
            merged_params: dict[str, Any] = {}
            for field_name, field_schema in pack_params.items():
                template_override = template.get('params', {}).get(field_name, {})
                merged_params[field_name] = _merge_param_schema(field_schema, template_override)
            for field_name, field_schema in template.get('params', {}).items():
                if field_name not in merged_params:
                    merged_params[field_name] = dict(field_schema)

            flattened[template_id] = {
                'description': template['description'],
                'params': merged_params,
                'pack': {
                    'operations_pack_id': pack['operations_pack_id'],
                    'version': pack['version'],
                    'description': pack['description'],
                },
                'compatibility': deepcopy(template.get('compatibility', pack_compat)),
            }
            template_to_pack[template_id] = pack

    return flattened, template_to_pack


_TEMPLATE_SCHEMAS, _TEMPLATE_TO_PACK = _flatten_template_packs(TEMPLATE_PACKS)


def normalize_and_validate_template_params(template_id: str, params: dict[str, Any] | None) -> dict[str, str]:
    spec = _TEMPLATE_SCHEMAS.get(template_id)
    if not spec:
        raise _unknown_template_error(template_id)

    incoming = dict(params or {})
    normalized: dict[str, str] = {}
    errors: list[dict[str, Any]] = []

    for field_name, field_schema in spec['params'].items():
        raw_value = incoming.get(field_name)
        field_path = f'params.{field_name}'
        if raw_value is None:
            if field_schema.get('required'):
                errors.append(
                    {
                        'path': field_path,
                        'reason': 'missing_required_field',
                        'expected': {'type': field_schema['type']},
                    }
                )
                continue
            raw_value = field_schema.get('default', '')

        if field_schema['type'] == 'string':
            if not isinstance(raw_value, str):
                errors.append(
                    {
                        'path': field_path,
                        'reason': 'invalid_type',
                        'expected': {'type': 'string'},
                        'actual_type': type(raw_value).__name__,
                    }
                )
                continue
            value = _normalize_string(raw_value)
            if field_name == 'job_name':
                try:
                    value = _normalize_job_name(value)
                except TemplateSchemaError as exc:
                    errors.extend(exc.errors)
                    continue
            if not value:
                errors.append(
                    {
                        'path': field_path,
                        'reason': 'empty_value',
                        'expected': {'type': 'string', 'min_length': 1},
                    }
                )
                continue
            normalized[field_name] = value
            continue

        errors.append(
            {
                'path': field_path,
                'reason': 'unsupported_schema_type',
                'expected': {'type': field_schema['type']},
            }
        )

    if errors:
        raise TemplateSchemaError(
            code='template_params_invalid',
            message='Template parameters failed validation',
            errors=errors,
        )

    return normalized


def get_template_schema(template_id: str, include_pack_metadata: bool = False) -> dict[str, Any]:
    spec = _TEMPLATE_SCHEMAS.get(template_id)
    if not spec:
        raise _unknown_template_error(template_id)

    payload = {
        'template_id': template_id,
        'description': spec['description'],
        'params': deepcopy(spec['params']),
    }
    if include_pack_metadata:
        payload['operations_pack'] = deepcopy(spec['pack'])
        payload['compatibility'] = deepcopy(spec.get('compatibility', {}))
    return payload


def get_template_catalog(
    include_pack_metadata: bool = False,
    grouped: bool = False,
) -> list[dict[str, Any]]:
    if grouped:
        packs: list[dict[str, Any]] = []
        for pack in TEMPLATE_PACKS:
            pack_payload = {
                'operations_pack_id': pack['operations_pack_id'],
                'version': pack['version'],
                'description': pack['description'],
                'compatibility': deepcopy(pack.get('compatibility', {})),
                'templates': [
                    get_template_schema(item['template_id'], include_pack_metadata=include_pack_metadata)
                    for item in pack.get('templates', [])
                ],
            }
            packs.append(pack_payload)
        return packs

    return [
        get_template_schema(template_id, include_pack_metadata=include_pack_metadata)
        for template_id in sorted(_TEMPLATE_SCHEMAS)
    ]
