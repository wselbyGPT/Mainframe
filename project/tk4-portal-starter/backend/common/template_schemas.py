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


TEMPLATE_SCHEMAS: dict[str, dict[str, Any]] = {
    'hello-world': {
        'description': 'Print a literal message to SYSOUT via IEBGENER.',
        'params': {
            'job_name': {
                'type': 'string',
                'required': False,
                'default': 'HELLO1',
                'format': 'jcl_job_name',
                'help': 'JCL job name (1-8 chars, starts with A-Z, then A-Z0-9#$@).',
                'examples': ['HELLO1', 'MYJOB99'],
            },
            'message': {
                'type': 'string',
                'required': False,
                'default': 'HELLO FROM WEB PORTAL',
                'help': 'Message line written to SYSUT1 in-stream data.',
                'examples': ['HELLO FROM THE WEB PORTAL'],
            },
        },
    },
    'idcams-listcat': {
        'description': 'Run IDCAMS LISTCAT for a catalog or level filter.',
        'params': {
            'job_name': {
                'type': 'string',
                'required': False,
                'default': 'LISTCAT',
                'format': 'jcl_job_name',
                'help': 'JCL job name (1-8 chars, starts with A-Z, then A-Z0-9#$@).',
                'examples': ['LISTCAT'],
            },
            'level': {
                'type': 'string',
                'required': True,
                'help': 'LISTCAT LEVEL operand (for example: SYS1 or USER.TEST).',
                'examples': ['SYS1', 'USER.TEST'],
            },
        },
    },
    'iebgener-copy': {
        'description': 'Copy one sequential dataset to another via IEBGENER.',
        'params': {
            'job_name': {
                'type': 'string',
                'required': False,
                'default': 'IEBGEN',
                'format': 'jcl_job_name',
                'help': 'JCL job name (1-8 chars, starts with A-Z, then A-Z0-9#$@).',
                'examples': ['COPYJOB'],
            },
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
    'sort-basic': {
        'description': 'Run SORT with a simple key on fixed records.',
        'params': {
            'job_name': {
                'type': 'string',
                'required': False,
                'default': 'SORTJOB',
                'format': 'jcl_job_name',
                'help': 'JCL job name (1-8 chars, starts with A-Z, then A-Z0-9#$@).',
                'examples': ['SORTJOB'],
            },
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
    'lattice-crypto-demo': {
        'description': 'Emit a lattice cryptography runbook stub to SYSOUT for mainframe operator workflows.',
        'params': {
            'job_name': {
                'type': 'string',
                'required': False,
                'default': 'LATTICE',
                'format': 'jcl_job_name',
                'help': 'JCL job name (1-8 chars, starts with A-Z, then A-Z0-9#$@).',
                'examples': ['LATTICE', 'PQCDEMO1'],
            },
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
}


def _unknown_template_error(template_id: str) -> UnknownTemplateError:
    supported = sorted(TEMPLATE_SCHEMAS)
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


def normalize_and_validate_template_params(template_id: str, params: dict[str, Any] | None) -> dict[str, str]:
    spec = TEMPLATE_SCHEMAS.get(template_id)
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


def get_template_schema(template_id: str) -> dict[str, Any]:
    spec = TEMPLATE_SCHEMAS.get(template_id)
    if not spec:
        raise _unknown_template_error(template_id)
    return {
        'template_id': template_id,
        'description': spec['description'],
        'params': deepcopy(spec['params']),
    }


def get_template_catalog() -> list[dict[str, Any]]:
    return [get_template_schema(template_id) for template_id in sorted(TEMPLATE_SCHEMAS)]
