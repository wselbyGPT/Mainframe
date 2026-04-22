from __future__ import annotations

from typing import Any, Callable

from common.template_schemas import (
    TemplateSchemaError,
    UnknownTemplateError,
    get_template_catalog,
    normalize_and_validate_template_params,
)

TemplateValidationError = TemplateSchemaError
TemplateRenderer = Callable[[dict[str, Any]], str]


class TemplateRenderError(ValueError):
    """Raised when template rendering cannot proceed."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _job_card(job_name: str, job_label: str) -> str:
    return f"//{job_name:<8} JOB ,'{job_label}',CLASS=A,MSGCLASS=H,MSGLEVEL=(1,1)"


def _exec(step_name: str, program: str) -> str:
    return f'//{step_name:<8} EXEC PGM={program}'


def _dd_sysout(name: str) -> str:
    return f'//{name:<8} DD SYSOUT=H'


def _dd_dataset(name: str, dsn: str, disp: str = 'SHR') -> str:
    return f'//{name:<8} DD DSN={dsn},DISP={disp}'


def _dd_instream(name: str) -> str:
    return f'//{name:<8} DD *'


def _dd_dummy(name: str = 'SYSIN') -> str:
    return f'//{name:<8} DD DUMMY'


def _instream_block(dd_name: str, lines: list[str]) -> list[str]:
    return [_dd_instream(dd_name), *lines, '/*']


def _header(job_name: str, label: str, step_name: str, pgm: str) -> list[str]:
    return [_job_card(job_name, label), _exec(step_name, pgm)]


def _iebgener_common_tail() -> list[str]:
    return [_dd_sysout('SYSUT2'), _dd_sysout('SYSPRINT'), _dd_dummy('SYSIN')]


def render_hello_world(params: dict[str, Any]) -> str:
    lines = [
        *_header(params['job_name'], 'WEB JOB', 'STEP1', 'IEBGENER'),
        *_instream_block('SYSUT1', [params['message']]),
        *_iebgener_common_tail(),
    ]
    return '\n'.join(lines)


def render_idcams_listcat(params: dict[str, Any]) -> str:
    lines = [
        *_header(params['job_name'], 'LISTCAT', 'STEP1', 'IDCAMS'),
        _dd_sysout('SYSPRINT'),
        *_instream_block('SYSIN', [f"  LISTCAT LEVEL('{params['level']}') ALL"]),
    ]
    return '\n'.join(lines)


def render_iebgener_copy(params: dict[str, Any]) -> str:
    lines = [
        *_header(params['job_name'], 'IEBGENER', 'COPY', 'IEBGENER'),
        _dd_dataset('SYSUT1', params['input_dataset']),
        _dd_dataset('SYSUT2', params['output_dataset']),
        _dd_sysout('SYSPRINT'),
        _dd_dummy('SYSIN'),
    ]
    return '\n'.join(lines)


def render_sort_basic(params: dict[str, Any]) -> str:
    lines = [
        *_header(params['job_name'], 'SORT', 'SORTSTEP', 'SORT'),
        _dd_dataset('SORTIN', params['input_dataset']),
        _dd_dataset('SORTOUT', params['output_dataset']),
        _dd_sysout('SYSOUT'),
        *_instream_block('SYSIN', [f"  SORT FIELDS=({params['sort_fields']})"]),
    ]
    return '\n'.join(lines)


def render_lattice_crypto_demo(params: dict[str, Any]) -> str:
    lines = [
        *_header(params['job_name'], 'PQC DEMO', 'PQCSTEP', 'IEBGENER'),
        *_instream_block(
            'SYSUT1',
            [
                'POST-QUANTUM CRYPTOGRAPHY EXECUTION PLAN',
                f"ALGORITHM={params['algorithm']}",
                f"SECURITY_LEVEL={params['security_level']}",
                f"KEY_DATASET={params['key_dataset']}",
                f"NOTES={params['notes']}",
            ],
        ),
        *_iebgener_common_tail(),
    ]
    return '\n'.join(lines)


TEMPLATE_REGISTRY: dict[str, TemplateRenderer] = {
    'hello-world': render_hello_world,
    'idcams-listcat': render_idcams_listcat,
    'iebgener-copy': render_iebgener_copy,
    'sort-basic': render_sort_basic,
    'lattice-crypto-demo': render_lattice_crypto_demo,
}


def validate_template_params(template_id: str, params: dict[str, Any]) -> dict[str, Any]:
    return normalize_and_validate_template_params(template_id, params)


def render_template(template_id: str, params: dict[str, Any]) -> str:
    renderer = TEMPLATE_REGISTRY.get(template_id)
    if not renderer:
        raise TemplateRenderError(code='unknown_template_id', message=f"Unknown template_id '{template_id}'")

    normalized_params = normalize_and_validate_template_params(template_id, params)
    return renderer(normalized_params)


__all__ = [
    'TemplateRenderError',
    'TemplateValidationError',
    'UnknownTemplateError',
    'get_template_catalog',
    'normalize_and_validate_template_params',
    'render_template',
    'validate_template_params',
]
