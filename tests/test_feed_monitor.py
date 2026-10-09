import gzip

from darwin_lakehouse.feed_monitor import summarise

TS_MESSAGE = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
    '<Pport xmlns="http://www.thalesgroup.com/rtti/PushPort/v16" '
    'xmlns:ns5="http://www.thalesgroup.com/rtti/PushPort/Forecasts/v3" ts="2026-10-09T21:39:28+01:00">'
    '<uR updateOrigin="TD"><TS rid="202610096718005" uid="C18005" ssd="2026-10-09">'
    '<ns5:Location tpl="WEEDON" wtp="21:36"><ns5:pass at="21:39" src="TD"/></ns5:Location>'
    '</TS></uR></Pport>')


def test_summarise_decodes_gzipped_push_port_message():
    assert summarise(gzip.compress(TS_MESSAGE.encode())) == "uR TS rid=202610096718005 origin=TD"
