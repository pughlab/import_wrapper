package UHN::Format;

use strict;
use warnings;

use File::Find;
use File::Spec;
use File::Basename;

use Moose::Role;

requires 'handles_source';

requires 'scan';

requires 'finish';

sub scan_paths {
  my ($self, $importer, $function, $pattern, $directory, @args) = @_;

  $directory = File::Spec->rel2abs($directory);
  return if (! -d $directory);

  my @result = ();
  my $caller = sub {
    my $file = $File::Find::name;
    $importer->logger()->info("Scanning $file");
    return if (! -f $file);
    return if (defined($pattern) && $file !~ $pattern);
    $importer->logger()->info("Processing $file");
    my ($name, $path, $suffix) = fileparse($file);
    my $value = &$function($self, $importer, $name, $file, @args);
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
