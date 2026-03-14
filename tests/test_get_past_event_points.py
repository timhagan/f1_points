import importlib


def test_main_handles_event_failures_and_reports_summary(monkeypatch, capsys):
    module = importlib.import_module('src.data_prep.get_past_event_points')

    monkeypatch.setattr(
        module.functions,
        'get_past_race_event_names',
        lambda: __import__('pandas').Series(['Race A', 'Race B'])
    )

    def fake_get_event_points(event_name):
        if event_name == 'Race B':
            raise RuntimeError('boom')

    monkeypatch.setattr(module.functions, 'get_event_points', fake_get_event_points)

    module.main()
    output = capsys.readouterr().out

    assert 'Skipping Race B: boom' in output
    assert 'Processed 2 past events (1 succeeded, 1 failed).' in output


def test_main_handles_missing_event_list(monkeypatch, capsys):
    module = importlib.import_module('src.data_prep.get_past_event_points')

    def raise_event_error():
        raise RuntimeError('schedule error')

    monkeypatch.setattr(module.functions, 'get_past_race_event_names', raise_event_error)

    module.main()
    output = capsys.readouterr().out

    assert 'Failed to load past events: schedule error' in output
    assert 'No past events found to process.' in output
