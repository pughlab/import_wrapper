package UHN::Commands::Command;

use strict;
use warnings;

use Moose::Role;

## Uses YAML::XS as a storable model to describe an individual command object.

require 'execute';

## The identity of the command. Can be used as a file name base for output
## and generated files.
require 'identity';

## The name of the command.
require 'name';

## The version of the command.
require 'version';

1;
