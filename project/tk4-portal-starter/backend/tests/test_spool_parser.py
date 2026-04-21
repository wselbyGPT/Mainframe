from common.spool_parser import split_spool, summarize_sections


def test_summarize_sections_tracks_lines_and_error_signals() -> None:
    sections = split_spool(
        """IEF236I ALLOC. FOR HELLO1 STEP1
//HELLO1 JOB (ACCT),'HELLO'
PRINT HELLO
STEP1 RC=0008
ABEND=S0C7"""
    )
    summary = summarize_sections(sections)

    assert summary['sections_total'] == 4
    assert summary['nonzero_rc_sections'] == 2
    assert summary['abend_sections'] == 2
    assert summary['section_types']['jes']['sections'] == 1
    assert summary['section_types']['raw']['lines'] == 5
