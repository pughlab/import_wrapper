use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/lib";

use Getopt::Long;
use Config::Any;
use Hash::Merge::Simple qw/merge/;
use File::Temp;
use File::Spec;
use File::Path qw(make_path);

use UHN::Importer;

use Log::Log4perl qw(:easy);
Log::Log4perl->init(\ <<'EOT');
  log4perl.category = DEBUG, Screen
  log4perl.appender.Screen = \
      Log::Log4perl::Appender::ScreenColoredLevels
  log4perl.appender.Screen.layout = \
      Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Screen.layout.ConversionPattern = \
      %p %F{1} %L> %m %n
EOT

my $logger = get_logger();

my $config = 'import.yml';
my $output = 'out';
my $help;

GetOptions(
  'help|?' => \$help,
  'config=s' => \$config,
  'output=s' => \$output,
) or die("Error in command line arguments\n");

if (! -e $config) {
  $logger->error("Can't find config file: $config");
  exit(1);
}

$logger->info("Reading config file: $config");
my $cfg = Config::Any->load_files({files => ['defaults.yml', 'local.yml', $config], use_ext => 1});

my @hashes = map {
  my ($key) = keys %$_;
  $_->{$key};
} @$cfg;

$cfg = Hash::Merge::Simple->merge(@hashes);
$cfg->{PERL_EXECUTABLE} = $^X;
$cfg->{LOGGER} = $logger;
$cfg->{TEMP_DIRECTORY} = File::Temp->newdir();

$output = File::Spec->rel2abs($output);
if (! -d $output) {
  make_path($output);
}
$cfg->{OUTPUT} = $output;

UHN::Importer::build_import($cfg);

pod2usage(1) if $help;

1;

__END__

=head1 NAME

import.pl - Script to import data into cBioPortal

=head1 SYNOPSIS

import.pl [options]
 Options:
   --help            brief help message
   --config file     load configuration from the given file

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=item B<-man>

Loads configuration from the given file.

=back

=head1 DESCRIPTION

B<This program> will read the given configuration file and build an
import directory for cBioPortal. This can be loaded using the Java
script runner.

=cut
