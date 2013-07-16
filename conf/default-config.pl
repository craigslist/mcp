server => {
  syslog_ident => 'mcpd',
},

poller => {
  epoll_create_number    => 20,
  epoll_event_chunk_size => 20,
  tick_milliseconds      => 100,
  read_buffer_chunk_size  => (16 * 1024),
  write_buffer_chunk_size => 0,
},

request_module_sequence => [qw/ 
  StatusPage Redirector HeaderStamper DirectCache Forwarder Exceptions Logger
/],

requests => {
  default_ports         => 80,
  acceptable_methods    => 'GET|POST|PUT|HEAD|PURGE',
  request_timeout       => 300,                # in seconds
  headers_timeout       => 20,                 # in seconds
  idle_timeout          => 300,                # in seconds
  max_length            => (16 * 1024 * 1024), # in bytes
  max_unbuffered_length => (16 * 1024),        # in bytes
  trusted_local_forwarders => {
    #'10.128.131.254' => 1,
  },
  #debug_dump_url        => 'frootbat', 
},

cache_server => {
  max_pending_gets    => 2500,
  max_get_queue_size  => 0,
  get_queue_timeout   => 0.200,        # in seconds, float ok
  nonblocking         => 1,            # use memcached in nonblocking mode?
  cache_read_methods  => 'GET|HEAD|POST',
  cache_write_methods => 'GET',
  hot_cache_refresh   => 10,           # refresh interval, in seconds
  default_page_expiration => 86400,    # in seconds
  #
  # For requests matching the following expressions, run the
  # associated subs on the cache key.  This allows us to do some
  # clever processing to use the same cache slot for content we know
  # to be invariant.  That is where we know the origin server will
  # return exactly the same content regardless of the provided
  # accept-encoding value(s).  One can also imagine this mechanism
  # being used to strip parameters off of some urls before caching.
  #
  cache_key_transformations => [
    [ qr#\.(?:jpg|jpeg|png|gif)(?:$|\?)#i,
      sub($$) {
	      my ($request, $default_key) = @_;
	      # put all of these requests into the same cache
	      # slot by ignoring the "zip" bit
	      my @key_bits = split(/:/, $default_key);
	      $key_bits[2] = 0;		# always store in the 'non-zip' slot
	      return join(':', @key_bits);
      } ],
  ],
},


forwarder => {
  origin_connect_timeout          => 3,    # in seconds
  origin_connect_attempts         => 3,
  host_max_confidence             => 1,
  host_min_confidence             => 0.008,
  host_confidence_decrease_factor => 0.5,
  host_confidence_increase_factor => 2,
  maintenance_mode                => 0,
  origin_connect_max_retries => {
    status      => '503',
    status_text => 'Service Unavailable',
    content     => "Unable to process request, please try again.\n",
  },
  origin_connect_max_retries_404 => {
    status      => '404',
    status_text => 'Not Found',
    content     => "File not found.\n",
  },
  origin_connect_maintenance_mode => {
    status      => '503',
    status_text => 'Service Unavailable',
    content     => "Server maintenance underway, please try again later.\n",
  },
},

header_stamper => {
  modify_request_headers => [
    # match hostpath  => { modify headers }
  ],
  modify_response_headers => [
    # match hostpath, => { modify headers }
  ],
},

exceptions => {
  client_idle_timeout => {
    status      => '408',
    status_text => 'Request Timeout',
    content     => "Time to perform request exceeded.  Please try again.\n",
  },
  failed_to_parse_request => {
    status      => '400',
    status_text => 'Bad Request',
    content     => "Your request couldn't be processed, it may be malformed.\n",
  },
  request_uri_too_long => {
    status      => '414',
    status_text => 'Request URI Too Long',
    content     => "Your request URI is to long.\n",
  },
  failed_to_parse_response => {
    status      => '503',
    status_text => 'Service Unavailable',
    content     => "Unable to process request, please try again.\n",
  },
  admin_port_required => {
    status      => '403',
    status_text => 'Forbidden',
    content     => "Administrative function not available on this port.\n",
  },
  max_request_length_exceeded => {
    status      => '413',
    status_text => 'Request Entity Too Large',
    content     => "Your request couldn't be processed, it is too large.\n",
  },
  request_url_not_modified => {
    status      => '304',
    status_text => 'Not Modified',
    content     => "",
  },
},

status_page => {
  page_header => q|
    <html>
      <head>
        <style type="text/css">
          div.masthead {
            text-align: right;
            border-bottom: 1px solid #bbb;
          }
          div.module_list {
            border-bottom: 1px dotted #eee;
          }
          #module_StatusPage {
            display: block;
          }
          div.module_status {
            display: none;
          }
          div.module_status_outer {
            padding: 8px;
          }
          div.toggletree {
            margin-left: 16px;
            padding: 2px;
          }
          div.toggletree_list {
            display: none;
            padding: 2px;
          }
          span.module_list_item {
            font-size: 12px;
            padding: 4px;
          }
          span.mcp {
            font-size: 24px;
            font-weight: bold;
            color: #888;
          }
          span.version {
            font-size: 7px;
            color: #444;
          }
          span.percentbar {
            min-width: 100px;
            border: 1px solid #888;
          }
          span.percentbar_p {
            border: 1px solid #0b0;
            background-color: #bfb;
            max-height: 8px;
          }
          span.percentbar_np {
            max-height: 8px;
          }
          span {
            font-family: sans-serif;
            font-size: 10px;
          }
          div {
            font-family: sans-serif;
            font-size: 10px;
            border: 0px;
          }
          table {
            background-color: #eef;
            border-top: 1px solid #bbb;
            border-bottom: 1px solid #bbb;
          }
          td {
            font-family: sans-serif;
            font-size: 10px;
            border-bottom: 1px solid #fff;
            padding: 2px;
          }
          h2 {
            font-family: sans-serif;
            font-size: 10px;
            font-weight: bold;
            color: #444;
          }
          input {
            border: 1px solid #00f;
          }
          small {
            font-size: 8px;
          }
        </style>
        <script type="text/javascript">
          var lastToggle = 'module_StatusPage';
          function toggleExclusive( targetId ) {
            if(document.getElementById) {
              target = document.getElementById( targetId );
              lastTarget = document.getElementById( lastToggle );
              lastTarget.style.display = "none";
              target.style.display = "block";
              lastToggle = targetId;
            }
          } 

          function toggle( targetId ){
            if (document.getElementById){
              target = document.getElementById( targetId );
              if (target.style.display != "block"){
                target.style.display = "block";
              } else {
                target.style.display = "none";
              }
            }
          } 
        </script>
      </head>
      <body>
        <div class="masthead">
          <span class="mcp">MCP</span><br>
          <span class="version">memcache proxy version 2.05</span>
        </div>
  |,
  page_footer => q|
      </body>
    </html>
  |,
},

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
    # e.g. tell rss feed aggregators to use the correct search url
    #[ qr|^[^/]+/cgi-bin/search|, qr|/cgi-bin|, '' ],
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
    # e.g. folks coming from scammer.com shouldn't be able to deep link
    #[ qr|scammer.com/|,       qr|craigslist.org/\w+.*$|, 'craigslist.org/' ],
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

