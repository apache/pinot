#!/usr/bin/perl

use strict;

#
# Usage  ./scripts/generate_ivy_files.pl 0.1.192 .
#
# This script expands the ivy templates for each published module and stores them in
# their target directories.
# The hudson release job performs the following steps
# 1. Checkout gitli branch
# 2. Bump up mvn release version.
# 3. Generate ivy files with the new version <=== This script
# 4. Run mvn clean install -DskipTests to build package
# 5. Publish packages using mvn/push-artifact commands.
# 6. Commit and push the new pom.xml files with updated version.
#

#
# Sub-routing to expand the template "${PROJECT_VERSION}" with the actual version
#
sub generate_ivy_templates
{
  my ($template_file, $output_dir,$fileName) = @_;
  print "moving file $template_file to $output_dir/$fileName";
  system("mkdir -p $output_dir");
  system("cp $template_file $output_dir/$fileName");
}

my $usage="$0 <ROOT_DIRECTORY>";

my $version=`grep "<version>" pom.xml | head -1 | sed -e 's/<version>//' | sed -e 's/<\\/version>//'`;

chomp($version);
$version =~ s/\s*//g;
print "Version is : $version";
my $template_dir = "ivy_templates";

my @projects = ("pinot-api","pinot-broker", "pinot-common","pinot-controller","pinot-core","pinot-hadoop","pinot-server","pinot-tools","pinot-transport","pinot-util");

foreach my $p (@projects) {
  print "\n working on project : $p \n";
  my $dir = $p . "/target";
  my $template = $template_dir . "/" . $p . "-" . $version . ".xml" ;
  my $fileName = $p . "-" . $version . ".ivy" ;
  generate_ivy_templates($template,$dir,$fileName);  
}


