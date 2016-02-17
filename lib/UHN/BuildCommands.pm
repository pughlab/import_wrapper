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
  my ($cfg, $function, $pattern, @directories) = @_;

  @directories = map {
    File::Spec->rel2abs($_);
  } @directories;

  @directories = grep { -d $_ } @directories;

  return () if (! @directories);

  my @result = ();
  my $caller = sub {
    my $file = $File::Find::name;
    return if (! -f $file);
    return if (defined($pattern) && $pattern !~ $file);
    my ($name, $path, $suffix) = fileparse($file);
    my $value = &$function($cfg, $name, $file);
    if (defined($value)) {
      push @result, $value;
    }
  };

  my $options = {
    wanted => $caller,
    no_chdir => 1,
    preprocess => sub { return sort @_ },
  };
  File::Find::find($caller, @directories);

  return @result;
}

1;
