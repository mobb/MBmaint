use strict;
use warnings;
use Test::More;


use Catalyst::Test 'dsm';
use dsm::Controller::dsm;

ok( request('/dsm')->is_success, 'Request should succeed' );
done_testing();
