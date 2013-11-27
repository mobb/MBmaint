use strict;
use warnings;

use dsm;

my $app = dsm->apply_default_middlewares(dsm->psgi_app);
$app;

