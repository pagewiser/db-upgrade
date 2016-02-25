# Database migration tool

Simple tool to upgrade database to current state.

## Requirements

* dibi
* kdyby/console

## Registration

Update your config file

    console:
        commands:
            - Pagewiser\Database\Migration\DatabaseUpgradeCommand

## Usage

    php index.php database:upgrade ./db

For more options type

    php www/index.php help database:upgrade
