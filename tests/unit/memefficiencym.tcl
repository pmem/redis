proc test_memory_efficiency {range} {
    r flushall
    set rd [redis_deferring_client]
    set base_mem [s used_memory]
    set written 0
    for {set j 0} {$j < 10000} {incr j} {
        set key key:$j
        set val [string repeat A [expr {int(rand()*$range)}]]
        $rd set $key $val
        incr written [string length $key]
        incr written [string length $val]
        incr written 2 ;# A separator is the minimum to store key-value data.
    }
    for {set j 0} {$j < 10000} {incr j} {
        $rd read ; # Discard replies
    }

    set current_mem [s used_memory]
    set used [expr {$current_mem-$base_mem}]
    set efficiency [expr {double($written)/$used}]
    return $efficiency
}

start_server {tags {"memefficiency"}} {
    foreach {size_range expected_min_efficiency} {
        32    0.15
        64    0.25
        128   0.35
        1024  0.75
        16384 0.82
    } {
        test "Memory efficiency with values in range $size_range" {
            set efficiency [test_memory_efficiency $size_range]
            assert {$efficiency >= $expected_min_efficiency}
        }
    }
}

if 1 {
    start_server {tags {"defrag"}} {
        if {[string match {*jemalloc*} [s mem_allocator]]} {
            test "Active defrag" {
                r config set activedefrag no
                r config set active-defrag-threshold-lower 5
                r config set active-defrag-ignore-bytes 15mb
                r config set maxmemory 30mb
                r config set maxmemory-policy allkeys-lru
                r debug populate 700000 qweqweqweqweqweqweqweqwe 150
                r debug populate 170000 qweqweqweqweqweqweqweqwe 300

                set frag [r debug frag]
                set fragM [r debug fragM]

                assert {$frag >= 40}
                assert {$fragM >= 60}

                r config set activedefrag yes
                after 20000 ;# wait for defrag
                set hits [s active_defrag_hits]
                # wait for the active defrag to stop working
                set tries 0
                while { True } {
                    incr tries
                    after 500
                    set prev_hits $hits
                    set hits [s active_defrag_hits]
                    if {$hits == $prev_hits} {
                        break
                    }
                    assert {$tries < 100}
                }
                after 10000
				
                # TODO: we need to expose more accurate fragmentation info
                # i.e. the allocator used and active pages
                # instead we currently look at RSS so we need to ask for purge
                #r memory purge
				#
				#Update:
				#its done by new commands debug frag and debug fragM, 
				#very precisely fragmentation indicators

                # Test the the fragmentation is lower and that the defragger
                # stopped working
                #set frag [s mem_fragmentation_ratio]
                #assert {$frag < 1.55}
                #set misses [s active_defrag_misses]
                #after 500
                #set misses2 [s active_defrag_misses]
                #assert {$misses2 == $misses}
                #disabled atm
				
                set frag [r debug frag]
                set fragM [r debug fragM]
                assert {$frag < 10}
                assert {$fragM < 40}
            }
        }
    }
}
