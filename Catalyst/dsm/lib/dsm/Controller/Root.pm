package dsm::Controller::Root;
use Moose;
use namespace::autoclean;
use lib "/Users/peter/Projects/MSI/LTER/MBmaint";
use MBmaint::DSmeta;

BEGIN { extends 'Catalyst::Controller' }

#
# Sets the actions in this controller to be registered with no prefix
# so they function identically to actions created in MyApp.pm
#
__PACKAGE__->config(namespace => '');

=encoding utf-8

=head1 NAME

dsm::Controller::Root - Root Controller for dsm

=head1 DESCRIPTION

[enter your description here]

=head1 METHODS

=head2 index

The root page (/)

=cut

sub index :Path :Args(0) {
    my ( $self, $c ) = @_;

    # Hello World
    $c->response->body( $c->welcome_message );
}

=head2 default

Standard 404 error page

=cut

sub default :Path {
    my ( $self, $c ) = @_;
    $c->response->body( 'Page not found' );
    $c->response->status(404);
}

sub exportXML :Local {
    # Typical URL: http://localhost:3000/dsm/exportXML?dsid=10&dsname=DataSetPersonnel
    #
    # Note that Catalyst automatically
    # puts extra information after the "/<controller_name>/<action_name/"
    # into @_.  The args are separated  by the '/' char on the URL.
    my ($self, $c) = @_;

    #$c->response->body('Matched exportXML::Controller::dsm in dsm.');

    my $verbose = 1;
    my $datasetId = $c->request->query_parameters->{'dsid'};
    my $datasetName = $c->request->query_parameters->{'task'};

    my $dsMeta = MBmaint::DSmeta->new();
    my $xmlStr = $dsMeta->exportXML($datasetName, $datasetId, $verbose);

    #$c->response->body('this is fun');

    $c->stash(xml => $xmlStr,
              template => 'outputXML.tt');

    # Set the mime type of the response document to XML
    ##$c->response->content_type('application/xml');

    $c->response->content_type('text/plain');

    # Disable caching for this page
    $c->response->header('Cache-Control' => 'no-cache');
}

=head2 end

Attempt to render a view, if needed.

=cut

sub end : ActionClass('RenderView') {}

=head1 AUTHOR

peter

=head1 LICENSE

This library is free software. You can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

__PACKAGE__->meta->make_immutable;

1;
