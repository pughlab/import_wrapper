use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/lib";
use lib "$FindBin::Bin/local/lib/perl5";

use Getopt::Long;
use Config::Any;
use Hash::Merge::Simple qw/merge/;
use File::Temp;
use File::Spec;
use File::Path qw(make_path remove_tree);
use Pod::Usage;

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
my $output = '';
my $overwrite;
my $dry_run;
my $help;

GetOptions(
  'help|?' => \$help,
  'config=s' => \$config,
  'output=s' => \$output,
  'overwrite!' => \$overwrite,
  'dry-run!' => \$dry_run,
) or die("Error in command line arguments\n");

if ($help) {
  pod2usage(1);
  exit(0);
}

if (! -e $config) {
  $logger->error("Can't find config file: $config");
  exit(1);
}

if (! $output) {
  $logger->error("Missing --output argument, please specify an output directory");
  exit(1);
}

$logger->info("Reading config file: $config");
my $cfg = Config::Any->load_files({files => ["$FindBin::Bin/defaults.yml", "$FindBin::Bin/local.yml", $config], use_ext => 1});

my @hashes = map {
  my ($key) = keys %$_;
  $_->{$key};
} @$cfg;

$cfg = Hash::Merge::Simple->merge(@hashes);
$cfg->{PERL_EXECUTABLE} = $^X;
$cfg->{LOGGER} = $logger;
$cfg->{TEMP_DIRECTORY} = File::Spec->tmpdir();
$cfg->{_dry_run} = $dry_run;

$output = File::Spec->rel2abs($output);
if (-d $output && $overwrite && ! $dry_run) {
  remove_tree($output);
}
if (! -d $output && ! $dry_run) {
  make_path($output);
}
if (! -d "$output/case_lists" && ! $dry_run) {
  make_path("$output/case_lists");
}

$cfg->{OUTPUT} = $output;

my $importer = UHN::Importer->new($cfg);
$importer->run();

1;

__END__

=head1 NAME

import.pl - Script to import data into cBioPortal

=head1 SYNOPSIS

import.pl [options]
 Options:
   --help                 brief help message
   --config file          load configuration from the given file
   --output directory     where to write the data for cBioPortal

=head1 OPTIONS

=over 8

=item B<--help>

Print a brief help message and exits.

=item B<--config>

Loads configuration from the given file.

=item B<--output>

where to write the data for cBioPortal. This is now a required
command line argument, so don't expect it to default to anything
useful.

=item B<--overwrite>

Forces the system to overwrite any existing files. By default, the
import wrapper leaves existing files untouched.

=back

=head1 DESCRIPTION

B<This program> will read the given configuration file and build an
import directory for cBioPortal. The resulting data can be loaded using the Java
script runner, which is a different story.

=cut
