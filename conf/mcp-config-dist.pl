
# set this to the location of the default-config.pl that comes with mcp
#include /path/to/default-config.pl

# basic server settings
server => {
  processes => [ 1 .. 4 ],
  user      => 'mcp',
  group     => 'www',
},

# ips,ports to listen on for user service and for admin access
listeners => [
  # 8080:  administrative port: purges, status page, etc
  {
    ip         => '127.0.0.1',
    port       => 8080,
    queue_size => 128,
    process    => '1',
    is_default => 1,
    is_admin   => 1,
  },

  # 80: service port for user requests
  {
    ip         => '127.0.0.1',
    port       => 80,
    queue_size => 128,
    process    => '1',
    is_default => 1,
    virt_port  => 80,
  },
],

# enabled request modules (see below for each module's configuration)
request_module_sequence => [qw/ 
  StatusPage Redirector HeaderStamper DirectCache Forwarder Exceptions Logger
/],

# configuration for request settings and defaults
requests => {
#  default_host          => 'www.craigslist.org',
},

# configuration for memcached cluster
cache_server => {
#  memcached_servers => join(';', map { my $h = $_; map { $h.':'.$_ }
#    (qw/ 11001 11002 11003 11004 11005
#         11006 11007 11008 11009 11010 /) }  # port numbers
#    (map { '10.12.13.'.$_ }
#    (qw/  1  2  3  4  5  6  7  8  9 10 
#         11 12 13 14 15 16 17 18 19 20
#         21 22 23 24 25 26 27 28 29 30
#         31 32 33 34 35 36            /) ) ), # host numbers

  # cache these things locally inside each mcpd proc. (must kick mcp to purge)
  hot_cache_match => [
  ],

  # always return 304 for HEAD requests matching these URLs
  blind_304_match => [
  ],

  # cache 404s for GET requests matching these URLs
  cache_404_match => [
  ],
},

# configuration for requests forwarded to origin servers
forwarder => {
  # when maintenance mode is enabled, pages will be served only out of cache
  maintenance_mode                => 0,

  # origin server pool configuration
  origin_pools => {
#    ordinary_webservers => [
#        '10.12.1.1:80',
#        '10.12.1.2:80',
#        '10.12.1.3:80',
#    ],
#    search_webservers => [
#        '10.12.2.1:80',
#        '10.12.2.2:80',
#    ],
#    accounts_webservers => [
#        '10.12.5.1:80',
#        '10.12.5.2:80',
#    ],
  },

  # rules for directing URLs to specific origin pools
  pool_match => [ #first matching pool will be used 
    # regexp url match                     pool
#    [ qr|^search.craigslist.org|,          'search_webservers'         ],
#    [ qr|^accounts.craigslist.org|,        'accounts_webservers'       ],
  ],

  # any URLs not matching regexps in pool_match will go to this pool
#  default_origin_pool => 'ordinary_webservers',
},

# configuration for modification of request/response headers
header_stamper => {
  # headers to modify on incoming requests
  modify_request_headers => [
    # match hostpath regexp  => { modify headers }
  ],

  # headers to modify on outgoing responses
  modify_response_headers => [
    # match hostpath regexp, => { modify headers }
#    [ qr|favicon.ico|, { 'Cache-control' => 'public, max-age=2592000' } ],
  ],
},

# configuration for redirects, rewrites, moves, etc
redirector => {
  # rewrites change the url before passing it to origin servers
  # format: [ match, replace this, with this ]
  rewrites => [
    # e.g. always serve /favicon.ico from www.craigslist.org
    #[ qr|favicon.ico$|, qr|.*|, 'www.craigslist.org/favicon.ico'        ], 
  ],

  # moves issue a 301 to clients
  # format: [ match, replace this, with this ]
  moves => [
    # e.g. tell people this url has moved to a new location
    #[ qr|^[^/]+/cgi-bin/search|, qr|/search|, '' ],
  ],

  # redirects issue a 302 to clients
  # format: [ match, replace this, with this ]
  redirects => [
    # e.g. craigslist.com,.net,.biz users need to be sent to craigslist.org 
    #[ qr|craigslist.com/|, qr|craigslist.com|, 'craigslist.org'           ], 
  ],

  # referer_redirects will send 302 based on matching referer url
  # format: [ referer url match, replace this, with this ]
  referer_redirects => [
    # e.g. folks coming from malicious.net shouldn't be able to deep link
    #[ qr|malicious.net/|,       qr|craigslist.org/\w+.*$|, 'craigslist.org/' ],
  ],

  # cookie_redirects send 302 on the basis of a cookie submitted with request
  # format { cookie_name => [ [ replace this, with this ], ... ], ... }
  cookie_redirects => {
    # e.g. if you request www.cl.*/ and have the cl_def_hp cookie, redirect
    #cl_def_hp => [
    #  [ qr|^www.craigslist.[a-z\.]+?/?$|,        qr|^www| ],
    #],
  },

  # cookie_rewrites modify url on the basis of a cookie submitted with request
  # before passing request to origin servers.  they do not cause a redirect
  # format { cookie_name => [ [ replace this, with this ], ... ], ... }
  # using the text 'COOKIE' within the "with this" string will swap in the
  # cookie value
  cookie_rewrites => {
    # e.g. if you request www.cl.*/ and have the cl_def_hp cookie, rewrite
    #cl_def_hp => [
    #  [ qr|^www.craigslist.[a-z\.]+?/?$|,        qr|^www| ],
    #],
  },
},

# configuration for access log destination
logger => {
  log_host_ip   => '127.0.0.1',
  log_host_port => 25678,
#  domain_sock   => '/var/run/mcp_log_sock',
},


