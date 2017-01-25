/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ly.stealth.mesos.kafka.scheduler.http

import javax.inject.{Inject, Singleton}
import javax.ws.rs.core.{Feature, FeatureContext, Form, MediaType}
import org.glassfish.hk2.api.{Factory, InjectionResolver, ServiceLocator, TypeLiteral}
import org.glassfish.hk2.utilities.binding.AbstractBinder
import org.glassfish.jersey.message.internal.MediaTypes
import org.glassfish.jersey.server.ContainerRequest
import org.glassfish.jersey.server.internal.inject.{AbstractContainerRequestValueFactory, AbstractValueFactoryProvider, MultivaluedParameterExtractor, MultivaluedParameterExtractorProvider, ParamInjectionResolver}
import org.glassfish.jersey.server.model.Parameter
import org.glassfish.jersey.server.spi.internal.ValueFactoryProvider

class BothParamFeature extends Feature {
  class BothParamResolverBinder extends AbstractBinder {
    override def configure(): Unit = {
      bind(classOf[BothParamInjectionResolver]).to(
        new TypeLiteral[InjectionResolver[BothParam]] {}).in(classOf[Singleton])
      bind(classOf[BothParamValueFactoryProvider]).to(
        classOf[ValueFactoryProvider]).in(classOf[Singleton])
    }
  }

  override def configure(context: FeatureContext): Boolean = {
    context.register(new BothParamResolverBinder(), 0)
    true
  }
}

class BothParamInjectionResolver
  extends ParamInjectionResolver[BothParam](classOf[BothParamValueFactoryProvider])


class BothParamValueFactoryProvider @Inject() (
  mepe: MultivaluedParameterExtractorProvider, locator: ServiceLocator
) extends AbstractValueFactoryProvider(mepe, locator, Parameter.Source.UNKNOWN) {

  class BothParamValueFactory(
    extractor: MultivaluedParameterExtractor[_],
    decode: Boolean
  ) extends AbstractContainerRequestValueFactory[Object] {
    private val FORM_CACHE_KEY = "ly.stealth.mesos.kafka.scheduler.http.FORM_CACHE_KEY"

    override def provide(): Object = {
      val request = getContainerRequest
      if (request.getUriInfo.getQueryParameters.containsKey(extractor.getName)) {
        extractor.extract(request.getUriInfo.getQueryParameters(decode)).asInstanceOf[Object]
      } else {
        val cachedForm = request.getProperty(FORM_CACHE_KEY).asInstanceOf[Form]
        val form =
          if (cachedForm != null)
            cachedForm
          else {
            val newForm = readForm(request)
            request.setProperty(FORM_CACHE_KEY, newForm)
            newForm
          }
          extractor.extract(form.asMap()).asInstanceOf[Object]
      }
    }

    private def readForm(request: ContainerRequest): Form = {
      if (MediaTypes.typeEqual(MediaType.APPLICATION_FORM_URLENCODED_TYPE, request.getMediaType)) {
        request.bufferEntity
        val form: Form =
          if (decode)
            request.readEntity(classOf[Form])
          else {
            request.readEntity(classOf[Form])
          }
        if (form == null)
          new Form
        else form
      }
      else
        new Form
    }
  }

  override def createValueFactory(parameter: Parameter): Factory[_] = {
    val paramName = parameter.getSourceName
    if (paramName == null || paramName.length == 0)
      null
    else {
      val e = get(parameter)
      if (e == null) {
        null
      } else {
        new BothParamValueFactory(e, !parameter.isEncoded)
      }
    }
  }
}

