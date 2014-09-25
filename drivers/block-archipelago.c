/*
 * Copyright 2013 GRNET S.A. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 *   1. Redistributions of source code must retain the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer.
 *   2. Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer in the documentation and/or other materials
 *      provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY GRNET S.A. ``AS IS'' AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL GRNET S.A OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and
 * documentation are those of the authors and should not be
 * interpreted as representing official policies, either expressed
 * or implied, of GRNET S.A.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#include <xseg/xseg.h>
#include <xseg/protocol.h>

#include "list.h"
#include "tapdisk.h"
#include "tapdisk-driver.h"
#include "tapdisk-interface.h"
#include "tapdisk-server.h"

#ifdef HACE_CONFIG_H
#include "config.h"
#endif

#define MAX_ARCHIPELAGO_REQS        TAPDISK_DATA_REQUESTS
#define MAX_ARCHIPELAGO_MERGED_REQS 32
#define MAX_MERGE_SIZE              524288

#define XSEG_TYPENAME               "posix"
#define XSEG_NAME                   "archipelago"
#define XSEG_PEERTYPENAME           "posixfd"

#define ARCHIPELAGO_DFL_MPORT       1001
#define ARCHIPELAGO_DFL_VPORT       501

struct tdarchipelago_request {
    td_request_t treq[MAX_ARCHIPELAGO_MERGED_REQS];
    int treq_count;
    int op;
    uint64_t offset;
    uint64_t size;
    void *buf;
    ssize_t result;
    struct list_head queue;
};

typedef struct AIORequestData {
    char *volname;
    off_t offset;
    ssize_t size;
    void *buf;
    int ret;
    int op;
    struct tdarchipelago_request *tdreq;
} AIORequestData;

typedef struct ArchipelagoThread {
    pthread_t request_th;
    pthread_cond_t request_cond;
    pthread_cond_t init_done_cond;
    pthread_mutex_t request_mutex;
    pthread_mutex_t init_done_mutex;
    bool is_signaled;
    bool is_running;
    bool init_done;
} ArchipelagoThread;


struct tdarchipelago_data {
    /* Archipelago Volume Name,segment name and Size */
    char *volname;
    char *segment_name;
    uint64_t size;

    /* Archipelago specific */
    struct xseg *xseg;
    struct xseg_port *port;
    xport srcport;
    xport mportno;
    xport vportno;
    bool assume_v0;
    uint64_t v0_size;

    /* Archipelago I/O Thread */
    ArchipelagoThread *io_thread;

    /* Requests Queue */
    struct list_head reqs_inflight;
    struct list_head reqs_free;
    struct tdarchipelago_request *req_deferred;
    struct tdarchipelago_request reqs[MAX_ARCHIPELAGO_REQS];
    int reqs_free_count;

    /* Flush event */
    int timeout_event_id;

    /* Inter-thread pipe */
    int pipe_fds[2];
    int pipe_event_id;

    /* Driver Stats */
    struct {
        int req_total;

        int req_issued;
        int req_issued_no_merge;
        int req_issued_forced;
        int req_issued_direct;
        int req_issued_timeout;

        int req_miss;
        int req_miss_op;
        int req_miss_ofs;
        int req_miss_buf;
    } stat;
};


static void tdarchipelago_finish_aiocb(AIORequestData *reqdata);
static int tdarchipelago_close(td_driver_t *driver);
static void tdarchipelago_pipe_read_cb(event_id_t eb, char mode, void *data);

static int strstart(const char *str, const char *val, const char **ptr)
{
    const char *p, *q;
    p = str;
    q = val;
    while (*q != '\0') {
        if (*p != *q)
            return 0;
        p++;
        q++;
    }
    if (ptr)
        *ptr = p;
    return 1;
}

static void req_fix_v0(struct tdarchipelago_data *prv, struct xseg_request *req)
{
    if (!prv->assume_v0) {
        return;
    }
    req->flags |= XF_ASSUMEV0;
    if (prv->v0_size != -1) {
        req->v0_size = prv->v0_size;
    }
}

static int wait_reply(struct tdarchipelago_data *prv, struct xseg_request *expected_req)
{
    struct xseg_request *req;
    xseg_prepare_wait(prv->xseg, prv->srcport);
    void *psd = xseg_get_signal_desc(prv->xseg, prv->port);
    while(1) {
        req = xseg_receive(prv->xseg, prv->srcport, X_NONBLOCK);
        if(req) {
            if( req != expected_req) {
                DPRINTF("wait_reply(): Unknown request.\n");
                xseg_put_request(prv->xseg, req, prv->srcport);
            } else if(!(req->state & XS_SERVED)) {
                DPRINTF("wait_reply(): Failed request.\n");
                return -1;
            } else {
                break;
            }
        }
        xseg_wait_signal(prv->xseg, psd, 1000000UL);
    }
    xseg_cancel_wait(prv->xseg, prv->srcport);
    return 0;
}

static void xseg_request_handler(void *data)
{
    struct tdarchipelago_data *th_data = (struct tdarchipelago_data *) data;
    void *psd = xseg_get_signal_desc(th_data->xseg, th_data->port);
    ArchipelagoThread *th = th_data->io_thread;

    pthread_mutex_lock(&th->init_done_mutex);
    th->init_done = true;
    pthread_cond_signal(&th->init_done_cond);
    pthread_mutex_unlock(&th->init_done_mutex);

    pthread_mutex_lock(&th->request_mutex);
    while(th->is_running) {
        struct xseg_request *req;
        xseg_prepare_wait(th_data->xseg, th_data->srcport);
        req = xseg_receive(th_data->xseg, th_data->srcport, X_NONBLOCK);
        if(req) {
            AIORequestData *reqdata;
            xseg_get_req_data(th_data->xseg, req, (void **)&reqdata);

            if (!(req->state & XS_SERVED)) {
                reqdata->ret = -1;
                tdarchipelago_finish_aiocb(reqdata);
                xseg_put_request(th_data->xseg, req, th_data->srcport);
                continue;
            }

            if(reqdata->op == TD_OP_READ)
            {
                void *data = xseg_get_data(th_data->xseg, req);
                memcpy(reqdata->buf, data, req->serviced);
                reqdata->ret = req->serviced;
                tdarchipelago_finish_aiocb(reqdata);
                xseg_put_request(th_data->xseg, req, th_data->srcport);
            } else if(reqdata->op == TD_OP_WRITE) {
                reqdata->ret = req->serviced;
                tdarchipelago_finish_aiocb(reqdata);
                xseg_put_request(th_data->xseg, req, th_data->srcport);
            }
        } else {
            xseg_wait_signal(th_data->xseg, psd, 1000000UL);
        }
        xseg_cancel_wait(th_data->xseg, th_data->srcport);
    }
    th->is_signaled = true;
    pthread_cond_signal(&th->request_cond);
    pthread_mutex_unlock(&th->request_mutex);
    pthread_exit(NULL);
}

static int get_image_info(struct tdarchipelago_data *td)
{
    uint64_t size;
    int r;

    int targetlen = strlen(td->volname);
    struct xseg_request *req = xseg_get_request(td->xseg, td->srcport, td->mportno, X_ALLOC);
    r = xseg_prep_request(td->xseg, req, targetlen, sizeof(struct xseg_reply_info));
    if(r < 0) {
        xseg_put_request(td->xseg, req, td->srcport);
        DPRINTF("get_image_info(): Cannot prepare request. Aborting...");
        goto err_exit;
    }

    char *target = xseg_get_target(td->xseg, req);
    memcpy(target, td->volname, targetlen);
    req->size = req->datalen;
    req->offset = 0;
    req->op = X_INFO;
    req_fix_v0(td, req);

    xport p = xseg_submit(td->xseg, req, td->srcport, X_ALLOC);
    if(p == NoPort) {
        xseg_put_request(td->xseg, req, td->srcport);
        DPRINTF("get_image_info(): Cannot submit request. Aborting...");
        goto err_exit;
    }
    xseg_signal(td->xseg, p);
    r = wait_reply(td, req);
    if(r) {
        xseg_put_request(td->xseg, req, td->srcport);
        DPRINTF("get_image_info(): wait_reply() error. Aborting...");
        goto err_exit;
    }
    struct xseg_reply_info *xinfo = (struct xseg_reply_info *) xseg_get_data(td->xseg, req);
    size = xinfo->size;
    xseg_put_request(td->xseg, req, td->srcport);
    td->size = size;
    return 0;

err_exit:
    return -EIO;
}

static void xseg_find_port(char *pstr, const char *needle, xport *port)
{
    char *a;
    char *dpstr = strdup(pstr);
    a = strtok(dpstr, needle);
    *port = (xport) atoi(a);
    free(dpstr);
}

static void xseg_find_segment(char *pstr, const char *needle,
                              char **segment_name)
{
    const char *a;
    if (strstart(pstr, needle, &a)) {
        if (strlen(a) > 0) {
            *segment_name = strdup(a);
        }
    }
}

static void find_v0_size(char *size_str, const char *needle, uint64_t *size)
{
    char *a;
    char *temp_str = strdup(size_str);
    a = strtok(size_str, needle);
    *size = (uint64_t) atol(a);
    free(temp_str);
}

static void parse_uri(struct tdarchipelago_data *prv, const char *s)
{
    int n=0, nn, i;
    char *tokens[7];

    char *ds = strdup(s);
    tokens[n] = strtok(ds, ":");
    prv->volname = malloc(strlen(tokens[n]) + 1);
    strcpy(prv->volname, tokens[n]);

    for(i = 0, nn = 0; s[i]; i++)
        nn += (s[i] == ':');
    /* FIXME: Protect tokens array overflow */
    if( nn > 6)
        i = 6;
    else
        i = nn;

    while(tokens[n] && n <= i) tokens[++n] = strtok(NULL, ":");

    for(nn = 0; nn <= i; nn++) {
        if(strstr(tokens[nn], "mport="))
            xseg_find_port(tokens[nn], "mport=", &prv->mportno);
        if(strstr(tokens[nn], "vport="))
            xseg_find_port(tokens[nn], "vport=", &prv->vportno);
        if(strstr(tokens[nn], "assume_v0"))
            prv->assume_v0 = 1;
        if(strstr(tokens[nn], "v0_size="))
            find_v0_size(tokens[nn], "v0_size=", &prv->v0_size);
        if(strstr(tokens[nn], "segment="))
            xseg_find_segment(tokens[nn], "segment=", &prv->segment_name);
    }

    if(!prv->assume_v0 && prv->v0_size != -1) {
        DPRINTF("archipelago_parse_uri(): Ignoring provided v0_size\n");
        prv->v0_size = -1;
    }
}

static int tdarchipelago_open(td_driver_t *driver, const char *name, td_flag_t flags)
{
    struct tdarchipelago_data *prv = driver->data;
    int i, retval;

    /* Init private structure */
    memset(prv, 0x00, sizeof(struct tdarchipelago_data));

    /*Default mapperd and vlmcd ports */
    prv->vportno = ARCHIPELAGO_DFL_VPORT;
    prv->mportno = ARCHIPELAGO_DFL_MPORT;
    prv->assume_v0 = 0;
    prv->v0_size = -1;

    INIT_LIST_HEAD(&prv->reqs_inflight);
    INIT_LIST_HEAD(&prv->reqs_free);

    for(i=0; i< MAX_ARCHIPELAGO_REQS; i++){
        INIT_LIST_HEAD(&prv->reqs[i].queue);
        list_add(&prv->reqs[i].queue, &prv->reqs_free);
    }

    prv->reqs_free_count = MAX_ARCHIPELAGO_REQS;

    prv->pipe_fds[0] = prv->pipe_fds[1] = prv->pipe_event_id = -1;
    prv->timeout_event_id = -1;

    /* Parse Archipelago Volume Name and XSEG mportno, vportno */
    parse_uri(prv, name);

    if (!prv->segment_name) {
        prv->segment_name = strdup(XSEG_NAME);
    }

    /* Inter-thread pipe setup */
    retval = pipe(prv->pipe_fds);
    if(retval) {
        DPRINTF("tdarchipelago_open(): Failed to create inter-thread pipe (%d)\n", retval);
        retval = -errno;
        goto err_exit;
    }
    prv->pipe_event_id = tapdisk_server_register_event(
            SCHEDULER_POLL_READ_FD,
            prv->pipe_fds[0],
            0,
            tdarchipelago_pipe_read_cb,
            prv);

    /* Archipelago context */
    if(xseg_initialize()) {
        DPRINTF("tdarchipelago_open(): Cannot initialize xseg.\n");
        retval = -EFAULT;
        goto err_exit;
    }
    prv->xseg = xseg_join(XSEG_TYPENAME, prv->segment_name, XSEG_PEERTYPENAME, NULL);
    if(!prv->xseg) {
        DPRINTF("tdarchipelago_open(): Cannot join segment.\n");
        retval = -EFAULT;
        goto err_exit;
    }

    prv->port = xseg_bind_dynport(prv->xseg);
    if(!prv->port) {
        DPRINTF("tdarchipelago_open(): Failed to bind port.\n");
        xseg_leave(prv->xseg);
        retval = -EFAULT;
        goto err_exit;
    }
    prv->srcport = prv->port->portno;
    xseg_init_local_signal(prv->xseg, prv->srcport);

    retval = get_image_info(prv);
    if(retval < 0) {
        xseg_quit_local_signal(prv->xseg, prv->srcport);
        xseg_leave_dynport(prv->xseg, prv->port);
        xseg_leave(prv->xseg);
        goto err_exit;
    }

    driver->info.sector_size = DEFAULT_SECTOR_SIZE;
    driver->info.size = prv->size >> SECTOR_SHIFT;
    driver->info.info = 0;

    /* Start XSEG Request Handler Threads */
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    prv->io_thread = (ArchipelagoThread *) malloc(sizeof(ArchipelagoThread));

    pthread_cond_init(&prv->io_thread->request_cond, NULL);
    pthread_cond_init(&prv->io_thread->init_done_cond, NULL);
    pthread_mutex_init(&prv->io_thread->request_mutex, NULL);
    pthread_mutex_init(&prv->io_thread->init_done_mutex, NULL);
    prv->io_thread->is_signaled = false;
    prv->io_thread->is_running = true;
    prv->io_thread->init_done = false;
    pthread_create(&prv->io_thread->request_th, &attr,
            (void *) xseg_request_handler,
            (void *) prv);

    pthread_mutex_lock(&prv->io_thread->init_done_mutex);
    while (!prv->io_thread->init_done) {
        pthread_cond_wait(&prv->io_thread->init_done_cond,
                          &prv->io_thread->init_done_mutex);
    }
    pthread_mutex_unlock(&prv->io_thread->init_done_mutex);
    pthread_attr_destroy(&attr);

    return 0;

err_exit:
    return retval;
}

static int tdarchipelago_close(td_driver_t *driver)
{
    struct tdarchipelago_data *prv = driver->data;
    int r, targetlen;

    if(prv->pipe_fds[0] >= 0) {
        close(prv->pipe_fds[0]);
        close(prv->pipe_fds[1]);
    }

    if(prv->pipe_event_id >= 0)
        tapdisk_server_unregister_event(prv->pipe_event_id);

    prv->io_thread->is_running = 0;

    pthread_mutex_lock(&prv->io_thread->request_mutex);
    while(!prv->io_thread->is_signaled)
        pthread_cond_wait(&prv->io_thread->request_cond, &prv->io_thread->request_mutex);
    pthread_mutex_unlock(&prv->io_thread->request_mutex);
    pthread_join(prv->io_thread->request_th, NULL);
    pthread_cond_destroy(&prv->io_thread->request_cond);
    pthread_mutex_destroy(&prv->io_thread->request_mutex);
    free(prv->io_thread);

    targetlen = strlen(prv->volname);
    struct xseg_request *req = xseg_get_request(prv->xseg, prv->srcport, prv->vportno, X_ALLOC);
    r = xseg_prep_request(prv->xseg, req, targetlen, 0);
    if(r < 0) {
        DPRINTF("tdarchipelago_close(): Cannot prepare close request.");
        goto err_exit;
    }

    char *target = xseg_get_target(prv->xseg, req);
    memcpy(target, prv->volname, targetlen);
    req->size = req->datalen;
    req->offset = 0;
    req->op = X_CLOSE;
    req_fix_v0(prv, req);

    xport p = xseg_submit(prv->xseg, req, prv->srcport, X_ALLOC);
    if(p == NoPort) {
        xseg_put_request(prv->xseg, req, prv->srcport);
        DPRINTF("tdarchipelago_close(): Cannot submit close request.");
        goto err_exit;
    }

    xseg_signal(prv->xseg, p);
    r = wait_reply(prv, req);
    if(r < 0)
        DPRINTF("tdarchipelago_close(): wait_reply() error.");

    xseg_put_request(prv->xseg, req, prv->srcport);

err_exit:
    xseg_quit_local_signal(prv->xseg, prv->srcport);
    xseg_leave_dynport(prv->xseg, prv->port);
    xseg_leave(prv->xseg);
    return 0;
}

static void tdarchipelago_finish_aiocb(AIORequestData *reqdata)
{
    int rv;
    struct tdarchipelago_request *req = reqdata->tdreq;
    struct tdarchipelago_data *prv = req->treq[0].image->driver->data;

    req->result = reqdata->ret;

    while(1) {
        rv = write(prv->pipe_fds[1], (void *)&req, sizeof(req));
        if(rv >= 0)
            break;
        if((errno != EAGAIN) && (errno != EINTR))
            break;
    }
    free(reqdata);
    if(rv <= 0)
        DPRINTF("tdarchipelago_finish_aiocb(): Failed to write to completion pipe\n");
}

static void tdarchipelago_pipe_read_cb(event_id_t eb, char mode, void *data)
{
    struct tdarchipelago_data *prv = data;
    struct tdarchipelago_request *req;
    char *p = (void *)&req;
    int retval, tr, i;

    for(tr=0; tr<sizeof(req);) {
        retval = read(prv->pipe_fds[0], p + tr, sizeof(req) - tr);
        if(retval == 0) {
            DPRINTF("tdarchipelago_pipe_read_cb(): Short read on completion pipe\n");
            break;
        }
        if(retval < 0) {
            if( (errno == EAGAIN) || (errno == EINTR))
                continue;
            break;
        }
        tr += retval;
    }

    if(tr != sizeof(req)) {
        DPRINTF("tdarchipelago_pipe_read_cb(): Read aborted on completion pipe\n");
        return;
    }

    for(i=0; i < req->treq_count; i++)
    {
        int err = req->result < 0 ? -EIO : 0;
        if(err < 0)
            DPRINTF("tdarchipelago_pipe_read_cb(): Error in req->result: %d\n", err);
        td_complete_request(req->treq[i], err);
    }

    list_move(&req->queue, &prv->reqs_free);
    prv->reqs_free_count++;
}

static int __archipelago_submit_request(struct tdarchipelago_data *prv,
                                        struct tdarchipelago_request *tdreq)
{
    int retval;
    char *data, *target;
    AIORequestData *reqdata;
    int targetlen = strlen(prv->volname);

    struct xseg_request *req = xseg_get_request(prv->xseg, prv->srcport, prv->vportno, X_ALLOC);
    if (!req) {
        DPRINTF("__archipelago_submit_request(): Cannot get xseg request.\n");
        return -1;
    }

    retval = xseg_prep_request(prv->xseg, req, targetlen, tdreq->size);
    if (retval < 0) {
        DPRINTF("__archipelago_submit_request(): Cannot prepare xseg request.\n");
        goto err_exit;
    }

    target = xseg_get_target(prv->xseg, req);
    if (!target) {
        DPRINTF("__archipelago_submit_request(): Cannot get xseg target.\n");
        goto err_exit;
    }

    memcpy(target, prv->volname, targetlen);
    req->size = tdreq->size;
    req->offset = tdreq->offset;

    switch (tdreq->op) {
        case TD_OP_READ:
            req->op = X_READ;
            break;
        case TD_OP_WRITE:
            req->op = X_WRITE;
            break;
    }
    req->flags |= XF_FLUSH;
    req_fix_v0(prv, req);

    reqdata = (AIORequestData *) malloc(sizeof(AIORequestData));
    if (!reqdata) {
        DPRINTF("__archipelago_submit_request(): Cannot allocate reqdata.\n");
        goto err_exit;
    }

    reqdata->volname= prv->volname;
    reqdata->offset = tdreq->offset;
    reqdata->size = tdreq->size;
    reqdata->buf = tdreq->buf;
    reqdata->tdreq = tdreq;
    reqdata->op = tdreq->op;

    xseg_set_req_data(prv->xseg, req, reqdata);
    if (tdreq->op == TD_OP_WRITE) {
        data = xseg_get_data(prv->xseg, req);
        if(!data) {
            DPRINTF("__archipelago_submit_request(): Cannot get xseg data.\n");
            goto err_exit;
        }
        memcpy(data, tdreq->buf, tdreq->size);
    }

    xport p = xseg_submit(prv->xseg, req, prv->srcport, X_ALLOC);
    if(p == NoPort) {
        DPRINTF("__archipelago_submit_request(): Cannot submit xseg req.\n");
        goto err_exit;
    }
    xseg_signal(prv->xseg, p);
    return 0;
err_exit:
    DPRINTF("__archipelago_submit_request(): submit error\n");
    xseg_put_request(prv->xseg, req, prv->srcport);
    return -1;
}

static int tdarchipelago_submit_request(struct tdarchipelago_data *prv,
        struct tdarchipelago_request *req)
{
    int retval, i;
    prv->stat.req_issued++;
    list_add_tail(&req->queue, &prv->reqs_inflight);

    switch(req->op) {
        case TD_OP_READ:
        case TD_OP_WRITE:
            retval = __archipelago_submit_request(prv, req);
            break;
        default:
            retval = -EINVAL;
    }

    if (retval < 0) {
        retval = -EIO;
        goto err;
    }

    return 0;

err:
    for(i=0; i < req->treq_count; i++)
        td_complete_request(req->treq[i], retval);
    return retval;
}

static void tdarchipelago_timeout_cb(event_id_t eb, char mode, void *data)
{
    struct tdarchipelago_data *prv = data;

    if(prv->req_deferred) {
        tdarchipelago_submit_request(prv, prv->req_deferred);
        prv->req_deferred = NULL;
        prv->stat.req_issued_timeout++;
    }

    tapdisk_server_unregister_event(eb);
    prv->timeout_event_id = -1;
}

static void tdarchipelago_queue_request(td_driver_t *driver, td_request_t treq)
{
    struct tdarchipelago_data *prv= driver->data;
    size_t size = treq.secs * driver->info.sector_size;
    uint64_t offset = treq.sec * (uint64_t)driver->info.sector_size;
    struct tdarchipelago_request *req;
    int merged = 0;

    /* Update stats */
    prv->stat.req_total++;

    if(prv->req_deferred) {
        struct tdarchipelago_request *dr = prv->req_deferred;

        if((dr->op == treq.op) &&
            ((dr->offset + dr->size) == offset) &&
                (((unsigned long)dr->buf + dr->size)==(unsigned long)treq.buf))
        {
                    dr->treq[dr->treq_count++] = treq;
                    dr->size += size;
                    merged = 1;
        } else {
                    prv->stat.req_miss++;
                    if(dr->op != treq.op)
                        prv->stat.req_miss_op++;
                    if((dr->offset + dr->size) != offset)
                        prv->stat.req_miss_ofs++;
                    if(((unsigned long)dr->buf + dr->size) != (unsigned long)treq.buf)
                        prv->stat.req_miss_buf++;
        }

        if(!merged || (size != (11 * 4096)) || //44k request
                (dr->size >= MAX_MERGE_SIZE) ||
                (dr->treq_count == MAX_ARCHIPELAGO_MERGED_REQS))
        {
            tdarchipelago_submit_request(prv, dr);
            prv->req_deferred = NULL;

            if(!merged)
                prv->stat.req_issued_no_merge++;
            else
                prv->stat.req_issued_forced++;
        }
    }


    if(!merged) {
        if(prv->reqs_free_count == 0) {
            td_complete_request(treq, -EBUSY);
            goto no_free;
        }
        req = list_entry(prv->reqs_free.next, struct tdarchipelago_request, queue);

        list_del(&req->queue);
        prv->reqs_free_count--;

        /* Fill request */
        req->treq_count = 1;
        req->treq[0] = treq;

        req->op = treq.op;
        req->offset = offset;
        req->size = size;
        req->buf = treq.buf;

        if ((size == (11 * 4096)) && (size < MAX_MERGE_SIZE)) {
            prv->req_deferred = req;
        } else {
            tdarchipelago_submit_request(prv, req);
            prv->stat.req_issued_direct++;
        }
    }
no_free:
    if(prv->req_deferred && (prv->timeout_event_id == -1)) {
        prv->timeout_event_id = tapdisk_server_register_event(
            SCHEDULER_POLL_TIMEOUT,
            -1,
            0,
            tdarchipelago_timeout_cb,
            prv
            );
    } else if(!prv->req_deferred && (prv->timeout_event_id != -1)) {
        tapdisk_server_unregister_event(prv->timeout_event_id);
        prv->timeout_event_id = -1;
    }
}

static int tdarchipelago_get_parent_id(td_driver_t *driver, td_disk_id_t *id)
{
    return TD_NO_PARENT;
}

static int tdarchipelago_validate_parent(td_driver_t *driver, td_driver_t *parent,
        td_flag_t flags)
{
    return -EINVAL;
}


static void tdarchipelago_stats(td_driver_t *driver, td_stats_t *st)
{
    struct tdarchipelago_data *prv = driver->data;
    tapdisk_stats_field(st, "req_free_count", "d", prv->reqs_free_count);
    tapdisk_stats_field(st, "req_total", "d", prv->stat.req_total);
    tapdisk_stats_field(st, "req_issued", "d", prv->stat.req_issued);
    tapdisk_stats_field(st, "req_issued_no_merge", "d", prv->stat.req_issued_no_merge);
    tapdisk_stats_field(st, "req_issued_forced", "d", prv->stat.req_issued_forced);
    tapdisk_stats_field(st, "req_issued_direct", "d", prv->stat.req_issued_direct);
    tapdisk_stats_field(st, "req_issued_timeout", "d", prv->stat.req_issued_timeout);
    tapdisk_stats_field(st, "req_miss", "d", prv->stat.req_miss);
    tapdisk_stats_field(st, "req_miss_op", "d", prv->stat.req_miss_op);
    tapdisk_stats_field(st, "req_miss_ofs", "d", prv->stat.req_miss_ofs);
    tapdisk_stats_field(st, "req_miss_buf", "d", prv->stat.req_miss_buf);
    tapdisk_stats_field(st, "max_merge_size", "d", MAX_MERGE_SIZE);
}

struct tap_disk tapdisk_archipelago = {
    .disk_type = "tapdisk_archipelago",
    .private_data_size = sizeof(struct tdarchipelago_data),
    .flags = 0,
    .td_open = tdarchipelago_open,
    .td_close= tdarchipelago_close,
    .td_queue_read = tdarchipelago_queue_request,
    .td_queue_write = tdarchipelago_queue_request,
    .td_get_parent_id = tdarchipelago_get_parent_id,
    .td_validate_parent = tdarchipelago_validate_parent,
    .td_debug = NULL,
    .td_stats = tdarchipelago_stats,
};
