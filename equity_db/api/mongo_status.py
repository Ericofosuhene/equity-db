import os


def open_mongo() -> None:
    """
    Opens up mongo to let us connect
    :return: None
    """
    os.system('launchctl start homebrew.mxcl.mongodb-community')


def close_mongo() -> None:
    """
    Closes the mongo process
    can free up significant amounts memory
    :return: None
    """
    os.system('launchctl stop homebrew.mxcl.mongodb-community')