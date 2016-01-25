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
  my ($cfg, $function, @directories) = @_;
  @directories = map {
    File::Spec->rel2abs($_);
  } @directories;

  my @result = ();
  my $caller = sub {
    my $file = $File::Find::name;
    return if (! -f $file);
    my ($name, $path, $suffix) = fileparse($file);
    my $value = &$function($cfg, $name, $file);
    if (defined($value)) {
      push @result, $value;
    }
  };

  my $options = {
    wanted => $caller,
    no_chdir => 1,
  };
  File::Find::find($caller, @directories);

  return @result;
}

1;
