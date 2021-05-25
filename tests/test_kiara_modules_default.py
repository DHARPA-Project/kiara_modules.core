#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `kiara_modules_default` package."""

import pytest  # noqa

import kiara_modules.core


def test_assert():

    assert kiara_modules.core.get_version() is not None
