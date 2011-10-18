/*
 * rlm_gearman.c
 * vim: set expandtab ai ts=4 sw=4: 
 *
 * Version:	$Id$
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program; if not, write to the Free Software
 *   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 *
 * Copyright 2000,2011  The FreeRADIUS server project
 *
 * Copyright 2011       Roelf Diedericks <roelf@neology.co.za>
 */

#include <freeradius-devel/ident.h>
RCSID("$Id$")

#include <freeradius-devel/radiusd.h>
#include <freeradius-devel/modules.h>

#include <json/json.h>
#include <libgearman/gearman.h>

#ifndef HAVE_PTHREAD_H
/*
 *      This is a lot simpler than putting ifdef's around
 *      every use of the pthread functions.
 */
#define pthread_mutex_lock(a)
#define pthread_mutex_trylock(a) (0)
#define pthread_mutex_unlock(a)
#define pthread_mutex_init(a,b)
#define pthread_mutex_destroy(a)
#else
#include    <pthread.h>
#endif



typedef struct gearman_conn {
    gearman_client_st *client;
    char        locked;
#ifdef HAVE_PTHREAD_H
    pthread_mutex_t mutex;
#endif
} GEARMAN_CONN;

/*
 *	Define a structure for our module configuration.
 */
typedef struct rlm_gearman_t {
	char		*servers;

    /* managed pool of gearman clients */
    int         num_conns;
    GEARMAN_CONN    *conns;

    /* Name of the gearman functions for each module method */
    char    *func_authorize;
    char    *func_authenticate;
    char    *func_accounting;
    char    *func_preacct;
    char    *func_checksimul;
    char    *func_pre_proxy;
    char    *func_post_proxy;
    char    *func_post_auth;
#ifdef WITH_COA
    char    *func_recv_coa;
    char    *func_send_coa;
#endif

#ifdef HAVE_PTHREAD_H
    pthread_mutex_t mutex;
#endif


	CONF_SECTION    *cs;
} rlm_gearman_t;

/*
 *	A mapping of configuration file names to internal variables.
 *
 *	Note that the string is dynamically allocated, so it MUST
 *	be freed.  When the configuration file parser re-reads the string,
 *	it free's the old one, and strdup's the new one, placing the pointer
 *	to the strdup'd string into 'config.string'.  This gets around
 *	buffer over-flows.
 */
static const CONF_PARSER module_config[] = {
  { "servers",  PW_TYPE_STRING_PTR, offsetof(rlm_gearman_t,servers), NULL,  "127.0.0.1"},
  { "conns",  PW_TYPE_INTEGER, offsetof(rlm_gearman_t,num_conns), NULL,  "1"},

  { "func_authorize", PW_TYPE_STRING_PTR,
      offsetof(rlm_gearman_t,func_authorize), NULL, NULL},
  { "func_authenticate", PW_TYPE_STRING_PTR,
      offsetof(rlm_gearman_t,func_authenticate), NULL, NULL},
  { "func_accounting", PW_TYPE_STRING_PTR,
      offsetof(rlm_gearman_t,func_accounting), NULL, NULL},
  { "func_preacct", PW_TYPE_STRING_PTR,
      offsetof(rlm_gearman_t,func_preacct), NULL, NULL},
  { "func_checksimul", PW_TYPE_STRING_PTR,
      offsetof(rlm_gearman_t,func_checksimul), NULL, NULL},
  { "func_pre_proxy", PW_TYPE_STRING_PTR,
      offsetof(rlm_gearman_t,func_pre_proxy), NULL, NULL},
  { "func_post_proxy", PW_TYPE_STRING_PTR,
      offsetof(rlm_gearman_t,func_post_proxy), NULL, NULL},
  { "func_post_auth", PW_TYPE_STRING_PTR,
      offsetof(rlm_gearman_t,func_post_auth), NULL, NULL},
#ifdef WITH_COA
  { "func_recv_coa", PW_TYPE_STRING_PTR,
      offsetof(rlm_gearman_t,func_recv_coa), NULL, NULL},
  { "func_send_coa", PW_TYPE_STRING_PTR,
      offsetof(rlm_gearman_t,func_send_coa), NULL, NULL},
#endif

  { NULL, -1, 0, NULL, NULL }		/* end the list */
};



static int gearman_init_conns(rlm_gearman_t *inst) 
{ 
    gearman_client_st *client;
    gearman_return_t ret;
    int i=0;

    if ((pthread_mutex_trylock(&inst->mutex) != 0)) {
        radlog(L_ERR,"GEARMAN: someone else is already initializing connections");
        /* someone else is already initializing the connections */
        return 0;
    }

    /* create the first client, the rest we will clone */
    client=gearman_client_create(NULL);
    ret=gearman_client_add_server(client, inst->servers, 0);
    if (ret!=GEARMAN_SUCCESS) {
        radlog(L_ERR,"GEARMAN:gearman_init_conns: client_add_server failed %s\n", gearman_client_error(client) );
    }
    inst->conns[0].client=client;
    inst->conns[0].locked = 0;

    for(i=1;i<inst->num_conns;i++){
        radlog(L_DBG,"GEARMAN:gearman_init_conns: cloning pooled client id:%d",i);

        client=gearman_client_clone(NULL,inst->conns[0].client);
        if (!client || ret!=GEARMAN_SUCCESS) {
            radlog(L_ERR,"GEARMAN:gearman_init_conns: client_clone failed %s\n",gearman_client_error(client) );
            pthread_mutex_unlock(&(inst->mutex));
            return 0;
        }

        inst->conns[i].client=client;
        inst->conns[i].locked = 0;
    }
    pthread_mutex_unlock(&(inst->mutex));
    return 1;
}


/* acquire a gearman client from the pool */
static inline int gearman_get_conn(GEARMAN_CONN **ret, rlm_gearman_t *inst)
{
    register int i = 0;

    /* we initialize the gearman connection pool upon the first request
     * for a gearman client from the pool (via do_gearman) if the pool is empty
     */

    if (!inst->conns[0].client) {
        radlog(L_DBG,"GEARMAN:gearman_get_conn: no client connections yet, initializing pool");
        if ( !gearman_init_conns(inst) ) {
            return -1;
        }
    }

    /* walk the connection pool and find a free client */
    for(i=0;i<inst->num_conns;i++){
        radlog(L_DBG,"GEARMAN:gearman_get_conn: checking pool client, id: %d",i);
        if ((pthread_mutex_trylock(&inst->conns[i].mutex) == 0)) {
            if (inst->conns[i].locked == 1) {
                /* client is already being used */
                pthread_mutex_unlock(&(inst->conns[i].mutex));
                continue;
            }
            /* found an unused client */
            *ret = &inst->conns[i];
            inst->conns[i].locked = 1;
            radlog(L_DBG,"GEARMAN:gearman_get_conn: got pool client, id: %d",i);
            return i;
        }
    }
    return -1;
}

static inline void gearman_release_conn(int i, rlm_gearman_t *inst)
{
    GEARMAN_CONN *conns = inst->conns;

    radlog(L_DBG,"GEARMAN:gearman_release_conn: release pool client, id: %d", i);
    conns[i].locked = 0;
    pthread_mutex_unlock(&(conns[i].mutex));
}


/*
 *	Only free memory we allocated.  The strings allocated via
 *	cf_section_parse() do not need to be freed.
 */
static int gearman_detach(void *instance)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;

    //todo: release client pool, etc.

    if (inst->conns) {
       //todo: free the connection pool
    }
    free(instance);
    return 0;
}

/*
 *	Instantiate the module
 */
static int gearman_instantiate(CONF_SECTION *conf, void **instance)
{
    rlm_gearman_t *inst;
    int i=0;

    /*
     *	Set up a storage area for instance data
     */
    inst = rad_malloc(sizeof(*inst));
    if (!inst) {
        return -1;
    }
    memset(inst, 0, sizeof(*inst));

    /*
     *	If the configuration parameters can't be parsed, then
     *	fail.
     */
    if (cf_section_parse(conf, inst, module_config) < 0) {
        free(inst);
        return -1;
    }

    if (!inst->servers) {
        radlog(L_ERR, "GEARMAN: Must specify a gearman server with servers=");
        gearman_detach(inst);
        return -1;
    }

    if (!inst->num_conns) {
        radlog(L_ERR, "GEARMAN: Must specify number of connections with conns=");
        gearman_detach(inst);
        return -1;
    }

    /* init the connection pool's mutexes */
    inst->conns = malloc(sizeof(*(inst->conns))*inst->num_conns);
    for(i=0;i<inst->num_conns;i++){
        inst->conns[i].client=NULL;
        inst->conns[i].locked = 0;
        pthread_mutex_init(&inst->conns[i].mutex, NULL);
    }

    if ( !gearman_init_conns(inst) ) {
        radlog(L_ERR,"GEARMAN:gearman_instantiate: unable to create client pool");
        return -1;
    }

    /* mutex for socket pool initialisation */
    pthread_mutex_init(&inst->mutex, NULL);


    /* save the config section */
    inst->cs = conf;
    *instance = inst;

    return 0;
}



/* 
 * dump a radius request section in plain text 
 */

static void gearman_dump(VALUE_PAIR *vp, const char *section) {

    VALUE_PAIR  *nvp, *vpa, *vpn;
    char	namebuf[256];
    const   char *name;
    char	buffer[1024];
    int	attr, len;
    int i=0;


    nvp = paircopy(vp);

    while (nvp != NULL) {
        name =  nvp->name;
        attr = nvp->attribute;
        vpa = paircopy2(nvp,attr);

        if (vpa->next) {
            /* an attribute with multiple values, turn the values into json array */
            vpn = vpa;
            i=0;
            while (vpn) {
                len = vp_prints_value(buffer, sizeof(buffer),
                        vpn, FALSE);
                radlog(L_DBG,"GEARMAN:DUMP: %s: %s[%d]=%s\n",section,name,i,buffer);
                vpn = vpn->next;
                i++;
            }
        } else {
            /* regular attribute with single value */
            if ((vpa->flags.has_tag) &&
                    (vpa->flags.tag != 0)) {
                snprintf(namebuf, sizeof(namebuf), "%s:%d",
                        nvp->name, nvp->flags.tag);
                name = namebuf;
            }

            len = vp_prints_value(buffer, sizeof(buffer),
                    vpa, FALSE);
            radlog(L_DBG,"GEARMAN:DUMP %s: %s=%s\n",section,name,buffer);
        }

        pairfree(&vpa);
        vpa = nvp; while ((vpa != NULL) && (vpa->attribute == attr))
            vpa = vpa->next;
        pairdelete(&nvp, attr);
        nvp = vpa;
    }
}

/* 
 * build a json object from a chain of pairs,
 * placing them into  and place into the json hash called 'section' 
 */

static void gearman_build_json_req(json_object *json_req, VALUE_PAIR *vp, const char *section) {

    VALUE_PAIR  *nvp, *vpa, *vpn;
    char	namebuf[256];
    const   char *name;
    char	buffer[1024];
    int	attr, len;
    int i=0;


    json_object *section_obj=json_object_new_object();


    nvp = paircopy(vp);

    while (nvp != NULL) {
        name =  nvp->name;
        attr = nvp->attribute;
        vpa = paircopy2(nvp,attr);

        if (vpa->next) {
            /* an attribute with multiple values, turn the values into json array */
            vpn = vpa;
            json_object *arr_obj=json_object_new_array();
            i=0;
            while (vpn) {
                len = vp_prints_value(buffer, sizeof(buffer),
                        vpn, FALSE);
                radlog(L_DBG,"GEARMAN: %s: %s[%d]=%s\n",section,name,i,buffer);
                json_object_array_add(arr_obj, json_object_new_string(buffer));
                vpn = vpn->next;
                i++;
            }
            json_object_object_add(section_obj, name, arr_obj);
        } else {
            /* regular attribute with single value */
            if ((vpa->flags.has_tag) &&
                    (vpa->flags.tag != 0)) {
                snprintf(namebuf, sizeof(namebuf), "%s:%d",
                        nvp->name, nvp->flags.tag);
                name = namebuf;
            }

            len = vp_prints_value(buffer, sizeof(buffer),
                    vpa, FALSE);
            radlog(L_DBG,"GEARMAN: %s: %s=%s\n",section,name,buffer);
            json_object_object_add(section_obj, name, json_object_new_string(buffer));
        }

        pairfree(&vpa);
        vpa = nvp; while ((vpa != NULL) && (vpa->attribute == attr))
            vpa = vpa->next;
        pairdelete(&nvp, attr);
        nvp = vpa;
    }

    /* add this section to the main json object */
    json_object_object_add(json_req, section, section_obj);
}



/*
 * verify that a json object is a string and save it in a VP
 */
static int gearman_pairadd_json_obj(VALUE_PAIR **vp, const char *key, json_object *obj, int operator) {
    const char *val;
    VALUE_PAIR *vpp;
    char buf[1024];

    if ( !obj ) {
        radlog(L_ERR,"GEARMAN:\tadd: ERROR: Attribute %s is empty",key);

        return 0;
    }

    if ( json_object_is_type(obj,json_type_string) ) {
        val = json_object_get_string(obj);
        vpp = pairmake(key, val, operator);
        if (vpp != NULL) {
            pairadd(vp, vpp);
            radlog(L_DBG,"GEARMAN:\tadd: Added pair %s = (string)%s", key, val);
            return 1;
        } else {
            radlog(L_DBG,
                    "GEARMAN:\tadd: ERROR: Failed to create pair %s = %s",
                    key, val);
        }

    } else if ( json_object_is_type(obj,json_type_int) ) {
        int json_int = json_object_get_int(obj);
        snprintf(buf,sizeof(buf),"%d",json_int);
        vpp = pairmake(key, buf, operator);
        if (vpp != NULL) {
            pairadd(vp, vpp);
            radlog(L_DBG,"GEARMAN:\tadd: Added pair %s = (int)%s", key, buf);
            return 1;
        } else {
            radlog(L_DBG,
                    "GEARMAN:\tadd: ERROR: Failed to create pair %s = %s",
                    key, val);
        }

    } else if ( json_object_is_type(obj,json_type_double) ) {
        double json_double = json_object_get_double(obj);
        snprintf(buf,sizeof(buf),"%f",json_double);
        vpp = pairmake(key, buf, operator);
        if (vpp != NULL) {
            pairadd(vp, vpp);
            radlog(L_DBG,"GEARMAN:\tadd: Added pair %s = (double)%s", key, buf);
            return 1;
        } else {
            radlog(L_DBG,
                    "GEARMAN:\tadd: ERROR: Failed to create pair %s = %s",
                    key, val);
        }


    } else {
        /* json_type_boolean,
           json_type_double,
           json_type_int,
           json_type_object,
           json_type_array,
           json_type_string
         */
        radlog(L_ERR,"GEARMAN:\tadd: ERROR: json object is not a string %s is of type %d",key, json_object_get_type(obj));
    } 
    return 0;
}


/* 
 * decodes a chain of AVPs from a json hash's top level 'section'
 */
static int gearman_get_json_avps(json_object *json_resp, VALUE_PAIR **vp, const char *section)
{
    int      ret=0, i=0;
    const char *key; 
    struct json_object *val, *arrval, *obj; 
    struct lh_entry *entry;

    /*
       SV       *res_sv, **av_sv;
       AV       *av;
       char     *key;
       I32      key_len, len, i, j;
     */

    *vp = NULL;

    /* find the section */
    obj=json_object_object_get(json_resp,section);
    if (!obj)
        return 0;

    radlog(L_DBG," GEARMAN:get_json_avps: ===== %s =====\n",section);       

    if (!json_object_is_type(obj,json_type_object)) {
        radlog(L_DBG,"GEARMAN:get_json_avps: '%s' section is not an object, type is %d", section, json_object_get_type(obj));
        return 0;
    }

    /* iterate the entire 'section' and create VP's */
    for ( entry = json_object_get_object(obj)->head;
            (entry ? (key = (const char*)entry->k, val = (struct json_object*)entry->v, entry) : 0);
            entry = (struct lh_entry*)entry->next ) {

        if (!val) {
            radlog(L_ERR,"GEARMAN: key=%s is empty, not adding",key);
            continue;
        }
        radlog(L_DBG,"GEARMAN: key=%s val=%s",key, json_object_get_string(val));

        if (json_object_is_type(val,json_type_array)) {
            /* an attribute with array of values  */
            for(i=0; i < json_object_array_length(val); i++) {
                arrval = json_object_array_get_idx(val, i);
                radlog(L_DBG,"GEARMAN:\tavp: %s[%d]=%s\n", key, i, json_object_get_string(arrval));
                ret = gearman_pairadd_json_obj(vp, key, arrval, T_OP_ADD)+ret;
            }
        } else {
            /* plain old attribute-value */
            radlog(L_DBG,"GEARMAN:\tavp: %s=%s\n", key, json_object_get_string(val));
            ret = gearman_pairadd_json_obj(vp, key, val, T_OP_EQ)+ret;
        }
    }
    radlog(L_DBG,"GEARMAN:get_json_avps: ----- set %d %s VPs -----\n",ret,section); 

    return ret;
}



/* 
 * The main Gearman handler.
 *
 * 1. we receive a request from one of the module callback handlers, 
 *    and encode it into json.
 * 2. we then find a free gearman client from the pool, and invoke gearman_execute
 * 3. wait until a response arrives back from gearman, and deserialize the json 
 * 4. we update the original request structure with the response data
 */
static int do_gearman(void *instance, REQUEST *request, const char *request_type, const char *gearman_function)
{

    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    int exitstatus=RLM_MODULE_UPDATED;
    json_object *json_req;
    json_object *statuscode;
    gearman_client_st *client;
    GEARMAN_CONN *conn;
    int conn_id=-1;
    VALUE_PAIR  *vp;

    radlog(L_DBG,"GEARMAN:do_gearman(%s): servers(%s)\n", request_type, inst->servers);


    if ( (conn_id=gearman_get_conn(&conn, inst)) == -1 ) {
        radlog(L_ERR,"GEARMAN: All gearman clients (%d) are in use, consider increasing conns=\n",inst->num_conns);
        return RLM_MODULE_FAIL;
    }
    client=conn->client;
    if (!client) {
        radlog(L_ERR,"GEARMAN: error, got an invalid client from the pool\n");
        return RLM_MODULE_FAIL;
    }

    json_req = json_object_new_object();

    /* add the request type */
    json_object_object_add(json_req,"type", json_object_new_string(request_type));

    gearman_build_json_req(json_req,request->config_items,"control");
    gearman_build_json_req(json_req,request->packet->vps,"request");
    gearman_build_json_req(json_req,request->reply->vps,"reply");

    if (request->proxy != NULL) {
        gearman_build_json_req(json_req,request->proxy->vps,"proxy");
    } 
    if (request->proxy_reply !=NULL) {
        gearman_build_json_req(json_req,request->proxy_reply->vps,"proxy_reply");
    }

    /* convert json object to string */
    const char *request_str=json_object_to_json_string(json_req);
    //int request_str_len=strlen(request_str);
    radlog(L_DBG,"GEARMAN: REQUEST-JSON=%s\n", request_str );

    /* send it to gearman */
    gearman_argument_t value=gearman_argument_make(0, 0, request_str, strlen(request_str));

    gearman_task_st *task= gearman_execute(client, 
            gearman_function, strlen(gearman_function),  /* function to invoke */
            NULL, 0,                    /* no unique value provided */
            NULL, 
            &value, 0);

    if (task == NULL) /* gearman_execute() can return NULL on error */
    {
        radlog(L_ERR,"GEARMAN: task execute error: %s\n", gearman_client_error(client));
        gearman_release_conn(conn_id,inst); 
        return RLM_MODULE_FAIL;
    }

    if (!gearman_success(gearman_task_return(task))) {
        radlog(L_ERR,"GEARMAN: task failed: %s\n", gearman_client_error(client));
        gearman_task_free(task);
        gearman_release_conn(conn_id,inst); 
        return RLM_MODULE_FAIL;
    }


    gearman_result_st *result= gearman_task_result(task);
    const char *gearman_response=gearman_result_value(result);
    if (!gearman_response) {
        return RLM_MODULE_NOOP;
    }
    radlog(L_DBG,"GEARMAN: Received response %s\n", gearman_response );


    /* now convert the string back */
    json_object *json_resp = json_tokener_parse(gearman_response);

    /* we're done with the gearman task*/
    gearman_task_free(task);

    /* release the connection back to the pool */
    gearman_release_conn(conn_id,inst); 

    if (is_error(json_resp)) {
        radlog(L_DBG,"GEARMAN: RESPONSE-JSON:unable to parse json response: %s\n", gearman_response );
        return RLM_MODULE_FAIL;
    }

    radlog(L_DBG,"GEARMAN: RESPONSE-JSON = %s\n", json_object_to_json_string(json_resp) );


    /* stick the gearman response 'sections' back into freeradius structs */
    vp = NULL;

    /* request section */
    if (gearman_get_json_avps(json_resp, &vp, "request") > 0) {
        pairfree(&request->packet->vps);
        request->packet->vps = vp;
        vp = NULL;

        /*
         *  Update cached copies
         */
        request->username = pairfind(request->packet->vps,
                PW_USER_NAME);
        request->password = pairfind(request->packet->vps,
                PW_USER_PASSWORD);
        if (!request->password)
            request->password = pairfind(request->packet->vps,
                    PW_CHAP_PASSWORD);
    }

    /* control section */
    if (gearman_get_json_avps(json_resp, &vp, "control") > 0) {
        pairfree(&request->config_items);
        request->config_items = vp;
        vp = NULL;
    }

    /* reply section */
    if (gearman_get_json_avps(json_resp, &vp, "reply") > 0) {
        pairfree(&request->reply->vps);
        request->reply->vps = vp;
        vp = NULL;
    }

    /* proxy section */
    if (request->proxy && 
            gearman_get_json_avps(json_resp, &vp, "proxy") > 0) {
        pairfree(&request->proxy->vps);
        request->proxy->vps = vp;
        vp = NULL;
    }

    /* proxy_reply section */
    if (request->proxy_reply && 
            gearman_get_json_avps(json_resp, &vp, "proxy_reply") > 0) {
        pairfree(&request->proxy_reply->vps);
        request->proxy_reply->vps = vp;
        vp = NULL;
    }

    /* see if we got an exitcode, must be an integer */
    radlog(L_DBG,"GEARMAN: examining status code\n");
    statuscode=json_object_object_get(json_resp,"statuscode");
    if (statuscode) {
        if ( json_object_is_type(statuscode,json_type_int) ) {
            exitstatus=json_object_get_int(statuscode);
            radlog(L_DBG,"GEARMAN:response statuscode=%d\n", exitstatus);
        } else {
            radlog(L_ERR,"GEARMAN:response statuscode is not an integer\n");
        }
    } else {
        /* default to 'updated' if statuscode was not explicitly supplied */
        exitstatus=RLM_MODULE_UPDATED;
        radlog(L_INFO," GEARMAN:response statuscode was not supplied, defaulting to %d (RLM_MODULE_UPDATED)\n", exitstatus);
    }

    /* free the response json object */
    json_object_put(json_resp); 

    /* free the request json object */
    json_object_put(json_req); 

    /* debug -- a dump of everything's new state */
    gearman_dump(request->config_items,"control");
    gearman_dump(request->packet->vps,"request");
    gearman_dump(request->reply->vps,"reply");
    if (request->proxy) 
        gearman_dump(request->proxy->vps,"proxy");
    if (request->proxy_reply) 
        gearman_dump(request->proxy_reply->vps,"proxy_reply");

    return exitstatus;
}


/* a bunch of stubs, so we can figure out what we're being called as */
static int gearman_authenticate(void *instance, REQUEST *request)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    if (inst->func_authenticate) {
        return do_gearman(instance,request,"authenticate",inst->func_authenticate);
    } else {
        radlog(L_DBG," GEARMAN:authenticate requested, but no gearman function defined");
        return RLM_MODULE_NOOP;
    }
}
static int gearman_authorize(void *instance, REQUEST *request)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    if (inst->func_authorize) {
        return do_gearman(instance,request,"authorize",inst->func_authorize);
    } else {
        radlog(L_DBG," GEARMAN:authorize requested, but no gearman function defined");
        return RLM_MODULE_NOOP;
    }
}
static int gearman_preacct(void *instance, REQUEST *request)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    if (inst->func_preacct) {
        return do_gearman(instance,request,"preacct",inst->func_preacct);
    } else {
        radlog(L_DBG," GEARMAN:preacct requested, but no gearman function defined");
        return RLM_MODULE_NOOP;
    }
}
static int gearman_accounting(void *instance, REQUEST *request)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    if (inst->func_accounting) {
        return do_gearman(instance,request,"accounting",inst->func_accounting);
    } else {
        radlog(L_DBG," GEARMAN:accounting requested, but no gearman function defined");
        return RLM_MODULE_NOOP;
    }

}
static int gearman_checksimul(void *instance, REQUEST *request)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    if (inst->func_checksimul) {
        return do_gearman(instance,request,"checksimul",inst->func_checksimul);
    } else {
        radlog(L_DBG," GEARMAN:checksimul requested, but no gearman function defined");
        return RLM_MODULE_NOOP;
    }
}
static int gearman_pre_proxy(void *instance, REQUEST *request)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    if (inst->func_pre_proxy) {
        return do_gearman(instance,request,"pre-proxy",inst->func_pre_proxy);
    } else {
        radlog(L_DBG," GEARMAN:pre-proxy requested, but no gearman function defined");
        return RLM_MODULE_NOOP;
    }
}
static int gearman_post_proxy(void *instance, REQUEST *request)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    if (inst->func_post_proxy) {
        return do_gearman(instance,request,"post-proxy",inst->func_post_proxy);
    } else {
        radlog(L_DBG," GEARMAN:post-proxy requested, but no gearman function defined");
        return RLM_MODULE_NOOP;
    }
}
static int gearman_post_auth(void *instance, REQUEST *request)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    if (inst->func_post_auth) {
        return do_gearman(instance,request,"post-auth",inst->func_post_auth);
    } else {
        radlog(L_DBG," GEARMAN:post-auth requested, but no gearman function defined");
        return RLM_MODULE_NOOP;
    }
}
static int gearman_recv_coa(void *instance, REQUEST *request)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    if (inst->func_recv_coa) {
        return do_gearman(instance,request,"recv-coa",inst->func_recv_coa);
    } else {
        radlog(L_INFO," GEARMAN:recv-coa requested, but no gearman function defined");
        return RLM_MODULE_NOOP;
    }

}
static int gearman_send_coa(void *instance, REQUEST *request)
{
    rlm_gearman_t *inst=(rlm_gearman_t *)instance;
    if (inst->func_send_coa) {
        return do_gearman(instance,request,"send-coa",inst->func_send_coa);
    } else {
        radlog(L_INFO," GEARMAN:send-coa requested, but no gearman function defined");
        return RLM_MODULE_NOOP;
    }
}


/*
 *	The module name should be the only globally exported symbol.
 *	That is, everything else should be 'static'.
 *
 *	If the module needs to temporarily modify it's instantiation
 *	data, the type should be changed to RLM_TYPE_THREAD_UNSAFE.
 *	The server will then take care of ensuring that the module
 *	is single-threaded.
 */
module_t rlm_gearman = {
	RLM_MODULE_INIT,
	"gearman",
	RLM_TYPE_THREAD_SAFE,		/* type, we work hard on the threadsafe bits */
	gearman_instantiate,		    /* instantiation */
	gearman_detach,			        /* detach */
	{
		gearman_authenticate,    /* authentication */
		gearman_authorize,       /* authorization */
		gearman_preacct,         /* preaccounting */
		gearman_accounting,      /* accounting */
		gearman_checksimul,      /* checksimul */
		gearman_pre_proxy,       /* pre-proxy */
		gearman_post_proxy,      /* post-proxy */
		gearman_post_auth        /* post-auth */
#ifdef WITH_COA
		,
        gearman_recv_coa,        /* recv-coa */
        gearman_send_coa         /* send-coa */
#endif
	},
};
