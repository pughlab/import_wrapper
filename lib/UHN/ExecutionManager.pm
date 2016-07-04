package UHN::ExecutionManager;

use Moose::Role;

use File::Temp qw/ tempfile tempdir /;
use Parallel::ForkManager;

requires 'cfg';
requires 'logger';

sub execute_commands {
  my ($self, $commands) = @_;
  my $cfg = $self->cfg();
  my $log = $self->logger();
  return if ($cfg->{_dry_run});

  my $pm = new Parallel::ForkManager($cfg->{max_processes});
  foreach my $command (@$commands) {
    next if ($command->executed());
    $command->execute($self, $pm) and next;
  }
  $pm->wait_all_children;

  $log->info("Done all commands");
}

1;
