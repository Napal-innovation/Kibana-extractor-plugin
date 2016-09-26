'use strict';

module.exports = function (kibana) {

    // Node modules
    const fs = require('fs')
    const path = require('path')
    const os = require('os')
    const crypto = require('crypto')
    const util = require('util')
    const _ = require('lodash')

    const getResultCleaner = (function() {
        var pool = {}
        const RESULT_TTL = 30 * 60 * 1000

        return ((fname) => {
            const remover = () => {
//                 console.log('Removing expired result: ' + fname)
                try {
                    fs.unlinkSync(fname)
                    delete pool[fname]
                } catch(e) {
                }
            }

            return new (function() {
                this.clear = () => {
                    if (fname in pool) {
                        clearTimeout(pool[fname])
                    }
                }

                this.update = () => {
                    if (fname in pool) {
                        clearTimeout(pool[fname])
                    }

                    pool[fname] = setTimeout(remover, RESULT_TTL)
                }
            })()
        })
    })()

    const getTempFilePath = function(hashname) {
        return util.format('%s%sesreply-%s.log', os.tmpdir(), path.sep, hashname)
    }

    const getReqWrapper = (function() {
            var pool = {}

            return ((hash) => {
                if (hash in pool) {
                    pool[hash]++
                } else {
                    pool[hash] = 1
                }

                return new (function(countval) {
                    const count = countval
                    var gcInterval

                    this.hashname = () => {
                        return util.format('%s-%d', hash, count)
                    }

                    this.filename = () => {
                        return getTempFilePath(this.hashname())
                    }

                    this.counter = () => {
                        return count
                    }

                    this.abort = false
                })(pool[hash])
            })
    })()

  return new kibana.Plugin({
    require: ['kibana', 'elasticsearch'],

    uiExports: {
      spyModes: ['plugins/kibana-extractor-plugin/downloadSpyMode']
    },

    init(server, options) {
        // Initialization goes here
        const esc = server.plugins.elasticsearch.client

        server.route({
            path: '/api/extractor/fields',
            method: 'GET',
            config: {
                timeout: {
                    socket: false
                },
            },
            handler(req, reply) {
                esc.indices.getMapping(
                    {
                        index: "logstash-*"
                    },
                    (err, response, status) => {
                        var fields = []

                        var props_process = (props, result, name) => {
                            _.forEach(props, (prop, pname) => {
                                if (prop.properties) {
                                    return props_process(prop.properties, result, !name ? pname : name + '.' + pname)
                                }
                                result.push(!name ? pname : name + '.' + pname)
                            })
                        }
                        _.forEach(response, (index) => {
                            _.forEach(index.mappings, (mapping) => {
                                props_process(mapping.properties, fields)
                            })
                        })

                        fields = _.uniq(fields)
                        reply({
                            fields: fields
                        })
                    }
                )
            }
        })

        server.route({
            path: '/api/extractor/index',
            method: 'POST',
            config: {
                timeout: {
                    socket: false
                },
            },
            handler(req, reply) {
                const PAGE_SIZE = 2000
                const SCROLL_TTL = '1m'
                const FILTER_FIELDS = ['message']

                var retr_stat

                var req_body = req.payload
                req_body.size = PAGE_SIZE
                if (!req_body.fields) {
                    req_body.fields = FILTER_FIELDS
                }

                var hash = crypto.createHash('sha256');
                hash.update(JSON.stringify(req_body))
                const fhash = hash.digest('hex')

                const wrapper = getReqWrapper(fhash);

                var abortSearch = () => {
//                     console.log('Aborted request ', fhash, ' #', wrapper.counter(), 'FROM', req.info.remoteAddress)
                    fs.unlinkSync(wrapper.filename())
                }

                var clientDisconnected = () => {
                    wrapper.abort = true
                }

                var dsv_escape = (val) => {
                    if (typeof(val) !== "string") {
                        return val
                    }

                    if (val.indexOf('"') > 0) {
                        val = val.replace('"', '""')
                    }

                    if (val.search(/["; \r\n\t]/) > 0) {
                        val = '"' + val + '"'
                    }

                    return val
                }

//                 console.log('SEARCH REQUEST ', fhash, ' #', wrapper.counter(), 'FROM', req.info.remoteAddress)
                esc.search(
                    {
                        scroll: SCROLL_TTL,
                        body: req_body
                    },
                    function getMoreUntilDone(error, response) {
                        if (wrapper.abort) {
                            abortSearch()
                            return;
                        }

                        if (error) {
                            console.warn(error)
                            return;
                        }

                        if (!retr_stat) {
                            retr_stat = {
                                totals: response.hits.total,
                                pages: Math.ceil(response.hits.total / PAGE_SIZE),
                                current: 1
                            }
                            try {
                                fs.unlinkSync(wrapper.filename())
                            } catch (e) {
                                // do nothing, continue
                            }

                            req.once('disconnect', clientDisconnected)
                            req.raw.req.on('aborted', clientDisconnected)
                        }


                        if (!retr_stat.totals) {
                            reply({
                                empty: true,
                            })
                            return
                        }

                        response.hits.hits.forEach((hit) => {
                            var write_str = ''

                            req_body.fields.forEach((field, idx) => {
                                if (idx) {
                                    write_str += ';'
                                }

                                if (field in hit.fields) {
                                    write_str += dsv_escape(hit.fields[field][0])
                                }
                            })

                            if (write_str) {
                                fs.appendFileSync(wrapper.filename(),
                                                  write_str + '\n')
                            }
                        })

                        if (retr_stat.current < retr_stat.pages) {
                            retr_stat.current += 1
//                             console.log('Scrolling to page: ', retr_stat.current, 'of', retr_stat.pages)
                            esc.scroll(
                                {
                                    scrollId: response._scroll_id,
                                    scroll: SCROLL_TTL
                                },
                                getMoreUntilDone
                            )
                            return
                        }

                        if (retr_stat.pages == retr_stat.current) {
                            getResultCleaner(wrapper.filename()).update()
                            reply({
                                empty: false,
                                link: util.format('/extractor/retrieve/%s',
                                                  wrapper.hashname()),
                            })
                        }
                    }
                )
            }
        })

        server.route({
            path: '/extractor/retrieve/{hash}',
            method: 'GET',
            handler(req, reply) {
                const response = req.response;
                var fname = getTempFilePath(req.params.hash)
                var fstream
                const rc = getResultCleaner(fname)

                try {
                    fs.accessSync(fname, fs.R_OK)
                    fstream = fs.createReadStream(fname)
                } catch(e) {
                    fstream = null
                }

                if (!fstream) {
                    reply('Log expired or does not exist').type('text/plain')
                    return
                }

                rc.clear()
                reply(fstream).type('text/plain').header(
                    'Content-Disposition',
                    'attachment; filename="extract.log"'
                ).once('finish', () => {
                    rc.update()
                })
            }
        })
    }

  });
};
