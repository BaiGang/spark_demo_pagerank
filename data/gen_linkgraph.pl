#!/usr/bin/perl
use strict;
use warnings;

my $num_pages = shift;

for (my $i = 1; $i <= $num_pages; ++$i) {
    print "$i ";
    my %hash = ();
    my $min = int(rand(4)) + 1;
    my $max = int(rand(20)) + $min;
    my $num_links = int(rand($max - $min)) + $min;
    for (my $j = 0; $j < $num_links; ++$j) {
        my $target = int(rand($num_pages)) + 1;
        $hash{$target} = 1;
    }
    foreach (keys %hash) {
      print "$_,";
    }
    print "\n";
}

