import json

from edgecitadel_agentd.trace_content import bounded_content, sanitize_event_content


def test_credentials_redacted_before_truncation_and_binary_omitted():
    content = bounded_content(
        {
            "arguments": {
                "API_KEY": "canary-one",
                "nested": {"authorization": "canary-two"},
                "text": "Authorization: Bearer canary-three https://user:canary-four@host/path?q=canary-five",
                "serialized": '{"refresh_token":"canary-six"}',
                "key": "-----BEGIN PRIVATE KEY-----\ncanary-seven\n-----END PRIVATE KEY-----",
                "binary": b"canary-eight",
                "large": "字" * 12000,
            }
        }
    )
    assert content["status"] == "truncated"
    assert (
        len(
            json.dumps(
                content["fields"], ensure_ascii=False, separators=(",", ":")
            ).encode()
        )
        <= 8192
    )
    assert "canary-" not in str(content)


def test_unsupported_or_recursive_content_fails_closed():
    cyclic = []
    cyclic.append(cyclic)
    for value in (object(), cyclic):
        assert bounded_content({"result": value}) == {
            "status": "unavailable",
            "reason": "redaction_failed",
            "fields": {},
        }


def test_journal_boundary_does_not_trust_producer_status():
    result = sanitize_event_content(
        {
            "content": {
                "status": "available",
                "fields": {"result": '{"password":"canary"}'},
            }
        }
    )
    assert "canary" not in str(result)
    assert "canary" not in str(bounded_content({"api_key": "canary"}))
