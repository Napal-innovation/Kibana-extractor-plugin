'use strict';

module.exports = function (kibana) {

    // Node modules
    const fs = require('fs')
    const crypto = require('crypto')
    const util = require('util')

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
                        return util.format('/tmp/esreply-%s.log', this.hashname())
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
        server.route({
            path: '/api/extractor/index',
            method: 'POST',
            config: {
                timeout: {
                    socket: false
                },
            },
            handler(req, reply) {
                const esc = server.plugins.elasticsearch.client
                const PAGE_SIZE = 2000
                const SCROLL_TTL = '1m'
                const FILTER_FIELDS = ['message']

                var retr_stat
                var req_body = req.payload
                req_body.size = PAGE_SIZE
                req_body.fields = FILTER_FIELDS

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
                            if (hit.fields[FILTER_FIELDS[0]]) {
                                fs.appendFileSync(wrapper.filename(), hit.fields[FILTER_FIELDS[0]] + '\n')
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
                                link: util.format('/extractor/retrieve/%s', wrapper.hashname()),
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
                var fname = util.format('/tmp/esreply-%s.log', req.params.hash)
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
