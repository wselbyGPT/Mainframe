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


def render_lattice_crypto_demo(params: dict[str, Any]) -> str:
    return '\n'.join([
        f"//{params['job_name']:<8} JOB ,'PQC DEMO',CLASS=A,MSGCLASS=H,MSGLEVEL=(1,1)",
        '//PQCSTEP  EXEC PGM=IEBGENER',
        '//SYSUT1   DD *',
        'POST-QUANTUM CRYPTOGRAPHY EXECUTION PLAN',
        f"ALGORITHM={params['algorithm']}",
        f"SECURITY_LEVEL={params['security_level']}",
        f"KEY_DATASET={params['key_dataset']}",
        f"NOTES={params['notes']}",
        '/*',
        '//SYSUT2   DD SYSOUT=H',
        '//SYSPRINT DD SYSOUT=H',
        '//SYSIN    DD DUMMY',
    ])


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
