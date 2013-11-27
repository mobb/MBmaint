package dsm::View::HTML;
use Moose;
use namespace::autoclean;

extends 'Catalyst::View::TT';

__PACKAGE__->config(
    #ABSOLUTE => 1,
    TEMPLATE_EXTENSION => '.tt',
    render_die => 1,
);

=head1 NAME

dsm::View::HTML - TT View for dsm

=head1 DESCRIPTION

TT View for dsm.

=head1 SEE ALSO

L<dsm>

=head1 AUTHOR

peter

=head1 LICENSE

This library is free software. You can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

1;
