# -*- coding: utf-8 -*-

import json

import requests

from gnippy import config
from gnippy.errors import *


def _generate_rules_url(url):
    """ Generate a rules URL from a PowerTrack URL """
    if ".json" not in url:
        raise BadPowerTrackUrlException("Doesn't end with .json")
    if "stream.gnip.com" not in url:
        raise BadPowerTrackUrlException("Doesn't contain stream.gnip.com")

    return url.replace(".json", "/rules.json").replace("stream.gnip.com", "api.gnip.com")


def _generate_post_object(rules_list):
    """ Generate the JSON object that gets posted to the Rules API. """
    if isinstance(rules_list, list):
        return { "rules": rules_list }
    else:
        raise BadArgumentException("rules_list must be of type list")


def _check_rules_list(rules_list):
    """ Checks a rules_list to ensure that all rules are in the correct format. """
    def fail():
        msg = "rules_list is not in the correct format. Please use build_rule to build your rules list."
        raise RulesListFormatException(msg)

    if not isinstance(rules_list, list):
        fail()

    expected = ("value", "tag")
    for r in rules_list:
        if not isinstance(r, dict):
            fail()

        if "value" not in r:
            fail()

        if not isinstance(r['value'], basestring):
            fail()

        if "tag" in r and not isinstance(r['tag'], basestring):
            fail()

        for k in r:
            if k not in expected:
                fail()


def _post(conf, built_rules):
    """
        Generate the Rules URL and POST data and make the POST request.
        POST data must look like:
        {
            "rules": [
                        {"value":"rule1", "tag":"tag1"},
                        {"value":"rule2"}
                     ]
        }

        Args:
            conf: A configuration object that contains auth and url info.
            built_rules: A single or list of built rules.
    """
    _check_rules_list(built_rules)
    rules_url = _generate_rules_url(conf['url'])
    post_data = json.dumps(_generate_post_object(built_rules))
    r = requests.post(rules_url, auth=conf['auth'], data=post_data)
    if not r.status_code in range(200,300):
        error_text = "HTTP Response Code: %s, Text: '%s'" % (str(r.status_code), r.text)
        raise RuleAddFailedException(error_text)


def _delete(conf, built_rules):
    """
        Generate the Rules URL and make a DELETE request.
        DELETE data must look like:
        {
            "rules": [
                        {"value":"rule1", "tag":"tag1"},
                        {"value":"rule2"}
                     ]
        }

        Args:
            conf: A configuration object that contains auth and url info.
            built_rules: A single or list of built rules.
    """
    _check_rules_list(built_rules)
    rules_url = _generate_rules_url(conf['url'])
    delete_data = json.dumps(_generate_post_object(built_rules))
    r = requests.delete(rules_url, auth=conf['auth'], data=delete_data)
    if not r.status_code in range(200,300):
        error_text = "HTTP Response Code: %s, Text: '%s'" % (str(r.status_code), r.text)
        raise RuleDeleteFailedException(error_text)


def build(rule_string, tag=None):
    """
        Takes a rule string and optional tag and turns it into a "built_rule" that looks like:
        { "value": "rule string", "tag": "my tag" }
    """
    if rule_string is None:
        raise BadArgumentException("rule_string cannot be None")
    rule = { "value": rule_string }
    if tag:
        rule['tag'] = tag
    return rule


def add_rule(rule_string, tag=None, **kwargs):
    """ Synchronously add a single rule to GNIP PowerTrack. """
    conf = config.resolve(kwargs)
    rule = build(rule_string, tag)
    rules_list = [rule,]
    _post(conf, rules_list)


def add_rules(rules_list, **kwargs):
    """ Synchronously add multiple rules to GNIP PowerTrack in one go. """
    conf = config.resolve(kwargs)
    _post(conf, rules_list)


def get_rules(**kwargs):
    """
        Get all the rules currently applied to PowerTrack.
        Optional Args:
            url: Specify this arg if you're working with a PowerTrack connection that's not listed in your .gnippy file.
            auth: Specify this arg if you want to override the credentials in your .gnippy file.

        Returns:
            A list of currently applied rules in the form:
            [
                { "value": "(Hello OR World) AND lang:en" },
                { "value": "Hello", "tag": "mytag" }
            ]
    """
    conf = config.resolve(kwargs)
    rules_url = _generate_rules_url(conf['url'])

    def fail(reason):
        raise RulesGetFailedException("Could not get current rules for '%s'. Reason: '%s'" % (rules_url, reason))

    try:
        r = requests.get(rules_url, auth=conf['auth'])
    except Exception as e:
        fail(str(e))

    if r.status_code not in range(200,300):
        fail("HTTP Status Code: %s" % r.status_code)

    try:
        rules_json = r.json()
    except:
        fail("GNIP API returned malformed JSON")

    if "rules" in rules_json:
        return rules_json['rules']
    else:
        fail("GNIP API response did not return a rules object")


def delete_rule(rule_dict, **kwargs):
    """ Synchronously delete a single rule from GNIP PowerTrack. """
    conf = config.resolve(kwargs)
    rules_list = [rule_dict,]
    _delete(conf, rules_list)


def delete_rules(rules_list, **kwargs):
    """ Synchronously delete multiple rules from GNIP PowerTrack. """
    conf = config.resolve(kwargs)
    _delete(conf, rules_list)
