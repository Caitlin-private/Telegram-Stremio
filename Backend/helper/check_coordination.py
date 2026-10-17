"""Coordinate the manual DB/file-size pass with the background checker."""
_checker = None

def register_checker(checker):
    global _checker
    _checker = checker

def manual_check_started():
    if _checker is not None:
        _checker.manual_started()

def manual_check_finished(successful: bool):
    if _checker is not None:
        _checker.manual_finished(successful)


def automatic_success():
    from Backend.helper.check_history import check_history
    return check_history.automatic
