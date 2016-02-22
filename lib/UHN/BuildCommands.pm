package UHN::BuildCommands;

use strict;
use warnings;

use File::Find;
use File::Spec;
use File::Basename;

use Carp;

## Given a directory, a callback function can be used to crawl the directory
## and build a list of command objects for later execution. It's a light
## wrapper around File::Find.
##
## The passed function is called with a (base) file name and a file path. if it
## returns a value, we accumulate it into a list result.

sub scan_paths {
  my ($cfg, $function, $pattern, $directory, @args) = @_;

  $directory = File::Spec->rel2abs($directory);
  return if (! -f $directory);

  my @result = ();
  my $caller = sub {
    my $file = $File::Find::name;
    $cfg->{LOGGER}->info("Scanning $file");
    return if (! -f $file);
    return if (defined($pattern) && $file !~ $pattern);
    $cfg->{LOGGER}->info("Processing $file");
    my ($name, $path, $suffix) = fileparse($file);
    my $value = &$function($cfg, $name, $file, @args);
    if (defined($value)) {
      push @result, $value;
    }
  };

  my $options = {
    wanted => $caller,
    no_chdir => 1,
    preprocess => sub { return sort @_ },
  };
  File::Find::find($caller, $directory);

  return @result;
}

1;
