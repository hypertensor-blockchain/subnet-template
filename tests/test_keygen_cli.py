from subnet.cli.crypto import keygen


def test_keygen_cli_passes_key_type(monkeypatch, tmp_path):
    calls = []
    key_path = tmp_path / "rsa.key"

    def fake_store_private_key(path, key_type):
        calls.append((path, key_type))

    monkeypatch.setattr(keygen, "store_private_key", fake_store_private_key)
    monkeypatch.setattr("sys.argv", ["keygen", "--path", str(key_path), "--key-type", "rsa"])

    keygen.main()

    assert calls == [(str(key_path), "rsa")]


def test_keygen_cli_accepts_underscore_key_type(monkeypatch, tmp_path):
    calls = []
    key_path = tmp_path / "secp256k.key"

    def fake_store_private_key(path, key_type):
        calls.append((path, key_type))

    monkeypatch.setattr(keygen, "store_private_key", fake_store_private_key)
    monkeypatch.setattr("sys.argv", ["keygen", "--path", str(key_path), "--key_type", "secp256k"])

    keygen.main()

    assert calls == [(str(key_path), "secp256k")]
