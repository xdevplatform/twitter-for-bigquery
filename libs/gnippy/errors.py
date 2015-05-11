# -*- coding: utf-8 -*-

class ConfigFileNotFoundException(Exception):
    """ Raised when an invalid config_file_path argument was passed. """
    pass


class IncompleteConfigurationException(Exception):
    """ Raised when no .gnippy file is found. """
    pass


class BadArgumentException(Exception):
    """ Raised when an invalid argument is detected. """
    pass


class RuleAddFailedException(Exception):
    """ Raised when a rule add fails. """
    pass


class RulesListFormatException(Exception):
    """ Raised when rules_list is not in the correct format. """
    pass


class RulesGetFailedException(Exception):
    """ Raised when listing the current rule set fails. """
    pass


class BadPowerTrackUrlException(Exception):
    """ Raised when the PowerTrack URL looks like its incorrect. """
    pass


class RuleDeleteFailedException(Exception):
    """ Raised when a rule delete fails. """
    pass