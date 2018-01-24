/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include <sys/sysinfo.h>
#include <thrift/c_glib/server/thrift_multithreaded_server.h>
#include <thrift/c_glib/transport/thrift_transport_factory.h>
#include <thrift/c_glib/protocol/thrift_protocol_factory.h>
#include <thrift/c_glib/protocol/thrift_binary_protocol_factory.h>

G_DEFINE_TYPE(ThriftMultithreadedServer, thrift_multithreaded_server, THRIFT_TYPE_SERVER)

static void thrift_multithreaded_worker_function(gpointer *arg)
{
  ThriftMultithreadedPoolArg *input = NULL;
  ThriftTransport *transport = NULL;
  ThriftServer *server = NULL;
  ThriftTransport *input_transport = NULL, *output_transport = NULL;
  ThriftProtocol *input_protocol = NULL, *output_protocol = NULL;
  GError *process_error = NULL;

  if(arg == NULL) return;

  input = (ThriftMultithreadedPoolArg *)arg;
  transport = input->transport;
  server = input->server;

  input_transport = THRIFT_TRANSPORT_FACTORY_GET_CLASS (server->input_transport_factory)
                    ->get_transport (server->input_transport_factory, transport);

  output_transport = THRIFT_TRANSPORT_FACTORY_GET_CLASS (server->output_transport_factory)
                     ->get_transport (server->output_transport_factory, transport);

  input_protocol = THRIFT_PROTOCOL_FACTORY_GET_CLASS (server->input_protocol_factory)
                   ->get_protocol (server->input_protocol_factory, input_transport);

  output_protocol = THRIFT_PROTOCOL_FACTORY_GET_CLASS (server->output_protocol_factory)
                    ->get_protocol (server->output_protocol_factory, output_transport);

  while (THRIFT_PROCESSOR_GET_CLASS (server->processor)->process (server->processor,
                                                                  input_protocol,
                                                                  output_protocol,
                                                                  &process_error)
        && thrift_transport_peek (input_transport, &process_error))
  {
  }

  if (process_error != NULL)
  {
    g_message ("thrift_simple_server_serve: %s", process_error->message);
    g_clear_error (&process_error);

    /* Note we do not propagate processing errors to the caller as they
     * normally are transient and not fatal to the server */
  }

  /* TODO: handle exceptions */
  THRIFT_TRANSPORT_GET_CLASS (input_transport)->close (input_transport,
                                                       NULL);
  THRIFT_TRANSPORT_GET_CLASS (output_transport)->close (output_transport,
                                                        NULL);

  g_free(arg);
}

gboolean
thrift_multithreaded_server_serve (ThriftServer *server, GError **error)
{
  ThriftTransport *t = NULL;
  ThriftMultithreadedServer *tms = THRIFT_MULTITHREADED_SERVER(server);
  ThriftMultithreadedPoolArg *arg = g_malloc0_n(1, sizeof(ThriftMultithreadedPoolArg));
  GError *threadpool_error = NULL;


  g_return_val_if_fail (THRIFT_IS_MULTITHREADED_SERVER (server), FALSE);
  g_return_val_if_fail (tms->worker_pool != NULL, FALSE);

  if (thrift_server_transport_listen (server->server_transport, error))
  {
    tms->running = TRUE;
    while (tms->running == TRUE)
    {
      t = thrift_server_transport_accept (server->server_transport, error);

      if (t != NULL && tms->running)
      {
        arg->server = server;
        arg->transport = t;
        g_thread_pool_push( tms->worker_pool, arg, &threadpool_error);

        if (threadpool_error != NULL)
        {
          g_message ("thrift_multithreaded_server_serve: %s", threadpool_error->message);
          g_clear_error (&threadpool_error);
        }
      }
    }

    /* attempt to shutdown */
    THRIFT_SERVER_TRANSPORT_GET_CLASS (server->server_transport)
      ->close (server->server_transport, NULL);
  }

  /* Since this method is designed to run forever, it can only ever return on
   * error */
  return FALSE;
}

void
thrift_multithreaded_server_stop (ThriftServer *server)
{
  g_return_if_fail (THRIFT_IS_MULTITHREADED_SERVER (server));
  (THRIFT_MULTITHREADED_SERVER (server))->running = FALSE;
  g_thread_pool_free((THRIFT_MULTITHREADED_SERVER (server))->worker_pool, TRUE, FALSE);
}

static void
thrift_multithreaded_server_init (ThriftMultithreadedServer *tms)
{
  ThriftServer *server = THRIFT_SERVER(tms);
  gint cores_count = get_nprocs_conf();
  tms->running = FALSE;

  tms->worker_pool = g_thread_pool_new ((GFunc)thrift_multithreaded_worker_function,
                                        NULL,
                                        cores_count,
                                        TRUE,
                                        NULL);

  if (server->input_transport_factory == NULL)
  {
    server->input_transport_factory =
        g_object_new (THRIFT_TYPE_TRANSPORT_FACTORY, NULL);
  }
  if (server->output_transport_factory == NULL)
  {
    server->output_transport_factory =
        g_object_new (THRIFT_TYPE_TRANSPORT_FACTORY, NULL);
  }
  if (server->input_protocol_factory == NULL)
  {
    server->input_protocol_factory =
        g_object_new (THRIFT_TYPE_BINARY_PROTOCOL_FACTORY, NULL);
  }
  if (server->output_protocol_factory == NULL)
  {
    server->output_protocol_factory =
        g_object_new (THRIFT_TYPE_BINARY_PROTOCOL_FACTORY, NULL);
  }
}

/* initialize the class */
static void
thrift_multithreaded_server_class_init (ThriftMultithreadedServerClass *class)
{
  ThriftServerClass *cls = THRIFT_SERVER_CLASS(class);

  cls->serve = thrift_multithreaded_server_serve;
  cls->stop = thrift_multithreaded_server_stop;
}
