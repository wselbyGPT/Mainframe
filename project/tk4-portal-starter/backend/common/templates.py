from __future__ import annotations

import re
from typing import Any, Callable

JOB_NAME_PATTERN = re.compile(r'^[A-Z][A-Z0-9#$@]{0,7}$')


class TemplateValidationError(ValueError):
    """Raised when template parameters are missing or invalid."""


TemplateRenderer = Callable[[dict[str, Any]], str]


TEMPLATE_SPECS: dict[str, dict[str, Any]] = {
    'hello-world': {
        'description': 'Print a literal message to SYSOUT via IEBGENER.',
        'params': {
            'job_name': {
                'required': False,
                'default': 'HELLO1',
                'description': 'JCL job name (1-8 chars, starts with A-Z, then A-Z0-9#$@).',
            },
            'message': {
                'required': False,
                'default': 'HELLO FROM WEB PORTAL',
                'description': 'Message line written to SYSUT1 in-stream data.',
            },
        },
    },
    'idcams-listcat': {
        'description': 'Run IDCAMS LISTCAT for a catalog or level filter.',
        'params': {
            'job_name': {
                'required': False,
                'default': 'LISTCAT',
                'description': 'JCL job name (1-8 chars, starts with A-Z, then A-Z0-9#$@).',
            },
            'level': {
                'required': True,
                'description': 'LISTCAT LEVEL operand (for example: SYS1 or USER.TEST).',
            },
        },
    },
    'iebgener-copy': {
        'description': 'Copy one sequential dataset to another via IEBGENER.',
        'params': {
            'job_name': {
                'required': False,
                'default': 'IEBGEN',
                'description': 'JCL job name (1-8 chars, starts with A-Z, then A-Z0-9#$@).',
            },
            'input_dataset': {
                'required': True,
                'description': 'Input DSN to read (quoted in JCL).',
            },
            'output_dataset': {
                'required': True,
                'description': 'Output DSN to write (quoted in JCL).',
            },
        },
    },
    'sort-basic': {
        'description': 'Run SORT with a simple key on fixed records.',
        'params': {
            'job_name': {
                'required': False,
                'default': 'SORTJOB',
                'description': 'JCL job name (1-8 chars, starts with A-Z, then A-Z0-9#$@).',
            },
            'input_dataset': {
                'required': True,
                'description': 'Input DSN to sort (quoted in JCL).',
            },
            'output_dataset': {
                'required': True,
                'description': 'Output DSN for sorted records (quoted in JCL).',
            },
            'sort_fields': {
                'required': False,
                'default': '1,10,CH,A',
                'description': 'DFSORT SORT FIELDS expression.',
            },
        },
    },
}


def _normalize_job_name(raw_value: Any, template_id: str) -> str:
    candidate = str(raw_value).strip().upper()
    if not candidate:
        raise TemplateValidationError(
            f"Invalid parameter 'job_name' for template '{template_id}': value cannot be empty"
        )
    if len(candidate) > 8:
        raise TemplateValidationError(
            f"Invalid parameter 'job_name' for template '{template_id}': must be at most 8 characters"
        )
    if not JOB_NAME_PATTERN.fullmatch(candidate):
        raise TemplateValidationError(
            f"Invalid parameter 'job_name' for template '{template_id}': "
            'must start with A-Z and contain only A-Z, 0-9, #, $, @'
        )
    return candidate


def _require_non_empty(value: Any, *, template_id: str, name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise TemplateValidationError(
            f"Invalid parameter '{name}' for template '{template_id}': value cannot be empty"
        )
    return normalized


def validate_template_params(template_id: str, params: dict[str, Any]) -> dict[str, Any]:
    spec = TEMPLATE_SPECS.get(template_id)
    if not spec:
        supported = ', '.join(sorted(TEMPLATE_SPECS))
        raise TemplateValidationError(
            f"Unknown template_id '{template_id}'. Supported template_ids: {supported}"
        )

    incoming = dict(params or {})
    validated: dict[str, Any] = {}

    for name, param_spec in spec['params'].items():
        if name in incoming and incoming[name] is not None:
            value = incoming[name]
        elif param_spec.get('required'):
            raise TemplateValidationError(
                f"Missing required parameter '{name}' for template '{template_id}'"
            )
        else:
            value = param_spec.get('default')

        if name == 'job_name':
            validated[name] = _normalize_job_name(value, template_id)
        else:
            validated[name] = _require_non_empty(value, template_id=template_id, name=name)

    return validated


def render_hello_world(params: dict[str, Any]) -> str:
    return '\n'.join([
        f"//{params['job_name']:<8} JOB ,'WEB JOB',CLASS=A,MSGCLASS=H,MSGLEVEL=(1,1)",
        '//STEP1    EXEC PGM=IEBGENER',
        '//SYSUT1   DD *',
        params['message'],
        '/*',
        '//SYSUT2   DD SYSOUT=H',
        '//SYSPRINT DD SYSOUT=H',
        '//SYSIN    DD DUMMY',
    ])


def render_idcams_listcat(params: dict[str, Any]) -> str:
    return '\n'.join([
        f"//{params['job_name']:<8} JOB ,'LISTCAT',CLASS=A,MSGCLASS=H,MSGLEVEL=(1,1)",
        '//STEP1    EXEC PGM=IDCAMS',
        '//SYSPRINT DD SYSOUT=H',
        '//SYSIN    DD *',
        f"  LISTCAT LEVEL('{params['level']}') ALL",
        '/*',
    ])


def render_iebgener_copy(params: dict[str, Any]) -> str:
    return '\n'.join([
        f"//{params['job_name']:<8} JOB ,'IEBGENER',CLASS=A,MSGCLASS=H,MSGLEVEL=(1,1)",
        '//COPY     EXEC PGM=IEBGENER',
        f"//SYSUT1   DD DSN={params['input_dataset']},DISP=SHR",
        f"//SYSUT2   DD DSN={params['output_dataset']},DISP=SHR",
        '//SYSPRINT DD SYSOUT=H',
        '//SYSIN    DD DUMMY',
    ])


def render_sort_basic(params: dict[str, Any]) -> str:
    return '\n'.join([
        f"//{params['job_name']:<8} JOB ,'SORT',CLASS=A,MSGCLASS=H,MSGLEVEL=(1,1)",
        '//SORTSTEP EXEC PGM=SORT',
        f"//SORTIN   DD DSN={params['input_dataset']},DISP=SHR",
        f"//SORTOUT  DD DSN={params['output_dataset']},DISP=SHR",
        '//SYSOUT   DD SYSOUT=H',
        '//SYSIN    DD *',
        f"  SORT FIELDS=({params['sort_fields']})",
        '/*',
    ])


TEMPLATE_REGISTRY: dict[str, TemplateRenderer] = {
    'hello-world': render_hello_world,
    'idcams-listcat': render_idcams_listcat,
    'iebgener-copy': render_iebgener_copy,
    'sort-basic': render_sort_basic,
}


def get_template_catalog() -> list[dict[str, Any]]:
    return [
        {
            'template_id': template_id,
            'description': TEMPLATE_SPECS[template_id]['description'],
            'params': TEMPLATE_SPECS[template_id]['params'],
        }
        for template_id in sorted(TEMPLATE_REGISTRY)
    ]


def render_template(template_id: str, params: dict[str, Any]) -> str:
    renderer = TEMPLATE_REGISTRY.get(template_id)
    if not renderer:
        supported = ', '.join(sorted(TEMPLATE_REGISTRY))
        raise ValueError(f"Unsupported template_id '{template_id}'. Supported template_ids: {supported}")

    validated_params = validate_template_params(template_id, params)
    return renderer(validated_params)
