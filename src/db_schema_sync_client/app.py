"""Application entrypoint."""

import sys

from db_schema_sync_client.paths import development_db_path


def main() -> int:
    from PyQt6.QtWidgets import QApplication, QDialog

    from db_schema_sync_client.infrastructure.app_store import AppStore
    from db_schema_sync_client.ui.login_dialog import LoginDialog
    from db_schema_sync_client.ui.main_window import MainWindow

    app = QApplication(sys.argv)

    # Load stylesheet
    import importlib.resources as _res

    try:
        style_text = _res.files("db_schema_sync_client.resources").joinpath("styles.qss").read_text()
        app.setStyleSheet(style_text)
    except Exception:
        pass

    db_path = development_db_path()
    from db_schema_sync_client.infrastructure.credentials import SQLiteCredentialStore
    credential_store = SQLiteCredentialStore(db_path)

    app_store = AppStore(db_path, credential_store=credential_store)
    app_store.initialize()

    login = LoginDialog(app_store)
    if login.exec() != QDialog.DialogCode.Accepted:
        return 0

    window = MainWindow(app_store)
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
