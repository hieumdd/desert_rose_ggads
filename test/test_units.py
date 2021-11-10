from unittest import mock

from main import main, UPLOAD_BUCKET


def test_print(capsys):
    file_name = "CALL_ID_SEARCH_TERM_VOID_OF_COSTS.csv"
    event = {
        "bucket": UPLOAD_BUCKET,
        "name": file_name,
        "metageneration": "some-metageneration",
        "timeCreated": "0",
        "updated": "0",
    }
    context = mock.MagicMock()
    context.event_id = "some-id"
    context.event_type = "gcs-event"
    
    main(event, context)
    out, _ = capsys.readouterr()
    assert file_name in out
