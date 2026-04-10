from __future__ import annotations

from typing import Any


def render_template(template_id: str, params: dict[str, Any]) -> str:
    if template_id != 'hello-world':
        raise ValueError(f'Unsupported template: {template_id}')

    job_name = str(params.get('job_name', 'HELLO1')).upper()[:8]
    message = str(params.get('message', 'HELLO FROM WEB PORTAL'))

    return "\n".join([
        f"//{job_name:<8} JOB ,'WEB JOB',CLASS=A,MSGCLASS=H,MSGLEVEL=(1,1)",
        "//STEP1    EXEC PGM=IEBGENER",
        "//SYSUT1   DD *",
        message,
        "/*",
        "//SYSUT2   DD SYSOUT=H",
        "//SYSPRINT DD SYSOUT=H",
        "//SYSIN    DD DUMMY",
    ])
