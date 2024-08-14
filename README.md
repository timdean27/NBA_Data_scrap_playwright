# Playwright API Testing for NBA Stats API

## Overview

This project provides automated tests for the NBA Stats API, which is a Django application offering endpoints for NBA player statistics. The tests utilize Playwright, a powerful end-to-end testing framework, to validate the functionality of key API endpoints. The primary focus of the tests is to ensure that the home page, player list, and player detail endpoints return the expected content.

## Features

- **Home Page Test:** Verifies that the home page contains the expected text `"Welcome to NBA Stats API"`.
- **Player List Test:** Checks if the player list page includes the text `"player"`.
- **Player Detail Test:** Confirms that the player detail page for a specific player ID contains `"id"`.
