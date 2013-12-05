/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using Org.Apache.Qpid.Messaging;

namespace Apache.NMS.Qpid
{
    /// <summary>
    /// Qpid provider of ISession
    /// </summary>
    public class Session : ISession
    {
        private Connection connection;
        private AcknowledgementMode acknowledgementMode;
        private IMessageConverter messageConverter;

        public Session(Connection connection, AcknowledgementMode acknowledgementMode)
        {
            this.connection = connection;
            this.acknowledgementMode = acknowledgementMode;
            MessageConverter = connection.MessageConverter;
            if(this.acknowledgementMode == AcknowledgementMode.Transactional)
            {
                // TODO: transactions
            }
        }

        public void Dispose()
        {
        }

        public IMessageProducer CreateProducer()
        {
            return CreateProducer(null);
        }

        public IMessageProducer CreateProducer(IDestination destination)
        {
            return new MessageProducer(this, (Destination) destination);
        }

        public IMessageConsumer CreateConsumer(IDestination destination)
        {
            return CreateConsumer(destination, null);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector)
        {
            return CreateConsumer(destination, selector, false);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector, bool noLocal)
        {
            if(selector != null)
            {
                throw new NotSupportedException("Selectors are not supported by Qpid");
            }
            return new MessageConsumer(this, acknowledgementMode);
        }

        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name, string selector, bool noLocal)
        {
            throw new NotSupportedException("TODO: Durable Consumer");
        }

        public void DeleteDurableConsumer(string name)
        {
            throw new NotSupportedException("TODO: Durable Consumer");
        }

        public IQueueBrowser CreateBrowser(IQueue queue)
        {
            throw new NotImplementedException();
        }

        public IQueueBrowser CreateBrowser(IQueue queue, string selector)
        {
            throw new NotImplementedException();
        }

        public IQueue GetQueue(string name)
        {
            return new Queue(name);
        }

        public ITopic GetTopic(string name)
        {
            throw new NotSupportedException("TODO: Topic");
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            throw new NotSupportedException("TODO: Temp queue");
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            throw new NotSupportedException("TODO: Temp topic");
        }

        /// <summary>
        /// Delete a destination (Queue, Topic, Temp Queue, Temp Topic).
        /// </summary>
        public void DeleteDestination(IDestination destination)
        {
            // TODO: Implement if possible.  If not possible, then change exception to NotSupportedException().
            throw new NotImplementedException();
        }

        public IMessage CreateMessage()
        {
            BaseMessage answer = new BaseMessage();
            return answer;
        }


        public ITextMessage CreateTextMessage()
        {
            TextMessage answer = new TextMessage();
            return answer;
        }

        public ITextMessage CreateTextMessage(string text)
        {
            TextMessage answer = new TextMessage(text);
            return answer;
        }

        public IMapMessage CreateMapMessage()
        {
            return new MapMessage();
        }

        public IBytesMessage CreateBytesMessage()
        {
            return new BytesMessage();
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            BytesMessage answer = new BytesMessage();
            answer.Content = body;
            return answer;
        }

        public IStreamMessage CreateStreamMessage()
        {
            return new StreamMessage();
        }

        public IObjectMessage CreateObjectMessage(Object body)
        {
            ObjectMessage answer = new ObjectMessage();
            answer.Body = body;
            return answer;
        }

        public void Commit()
        {
            throw new NotSupportedException("Transactions not supported by Qpid");
        }

        public void Rollback()
        {
            throw new NotSupportedException("Transactions not supported by Qpid");
        }

        public void Recover()
        {
            throw new NotSupportedException("Transactions not supported by Qpid");
        }

        // Properties
        public Connection Connection
        {
            get { return connection; }
        }

        /// <summary>
        /// The default timeout for network requests.
        /// </summary>
        public TimeSpan RequestTimeout
        {
            get { return NMSConstants.defaultRequestTimeout; }
            set { }
        }

        public IMessageConverter MessageConverter
        {
            get { return messageConverter; }
            set { messageConverter = value; }
        }

        public bool Transacted
        {
            get { return acknowledgementMode == AcknowledgementMode.Transactional; }
        }

        public AcknowledgementMode AcknowledgementMode
        {
            get { throw new NotImplementedException(); }
        }

        private ConsumerTransformerDelegate consumerTransformer;
        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get { return this.consumerTransformer; }
            set { this.consumerTransformer = value; }
        }

        private ProducerTransformerDelegate producerTransformer;
        public ProducerTransformerDelegate ProducerTransformer
        {
            get { return this.producerTransformer; }
            set { this.producerTransformer = value; }
        }

        public void Close()
        {
            Dispose();
        }

        #region Transaction State Events

        public event SessionTxEventDelegate TransactionStartedListener;
        public event SessionTxEventDelegate TransactionCommittedListener;
        public event SessionTxEventDelegate TransactionRolledBackListener;

        #endregion

    }
}
