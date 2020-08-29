# The Ingestion - Smartia Backend Developer Challenge

This repository contains the Smartia backend challenge, as well as instructions for setting up the environment.

## Setup
We provide a one-button setup using Vagrant and Virtualbox, which you are encouraged to use. You are
welcome to manually setup your environment if you prefer, but the Vagrant setup is similar to how we 
maintain integrity between local dev
and production environments here at Smartia. Having that integrity is
extremely important to us, as often the environment involves a mixture
of niche technology, maintaining of which manually is unreasonable.

### Vagrant
1. Install [Vagrant](https://www.vagrantup.com/) and 
[Virtualbox](https://www.virtualbox.org/wiki/Downloads)
2. Navigate into the project directory and run `vagrant up`
3. When the command is done, you are ready to go. You can either use [Vagrant
boxes with PyCharm](https://www.jetbrains.com/help/pycharm/vagrant-support.html)
which is akin to how we work, or `vagrant ssh` to work in the remote box
console. The choice is yours!

### Non-Vagrant
Check Setup section, and the content of `Vagrantfile` and good luck.

## Your Task

### Description
One of our employees just finished a tough period of integration with two
external services; one of which provides us with data from factory, and the 
other is a proprietary time series database. 

It is a working solution, but not the prettiest, sitting alone in `ingest.py`, 
completely away from our Django stack. We started to prepare tests in `features` 
following our favorite [BDD convention](https://behave.readthedocs.io/en/latest/). 
They are not implemented yet and they do not cover all the scenarios right now.

The reason for the integration with Django is that Django is where we keep all of our
data infrastructure and pipelines. It serves as focal point of our ecosystem,
and right now we have no plans to get away from it. We didn't include code for
that in the test as it is simply not needed. Just assume that this is a
big Django project already.

### The Required Code Part
For the code part of this task we have one requirement: we want you to convert the
`ingest.py` file into a Django command. A simple task, and a very simple test for
someone who has done similar work previously with Django. That is all the code you are
required to write for this challenge.

### The Fun Part
Now with the ingestion moved to a Django command, we need to whip this repository
into shape. Of course, we don't expect you to actually implement that, there
is so much work to be done here - from automating the Vagrant file further
(and possibly putting it into Ansible Playbooks), expanding `.feature` files to cover
all sorts of scenarios, to make the Django setup much more robust with logging, error
reporting and parallelization (one way or another, we will need the ingestion
code to scale both up-ways and side-ways) that asking you to implement even
one of those would be unfair. So we are not going to do that.

Instead, we want you to prepare a short presentation on how, given ownership of
this product, you would take it from where it is now to a production grade system,
ready and willing to process data in micro and macro scale. You can prepare this
presentation as pseudocode, diagrams, slides, sharpie notes written on your hand - it 
does not matter as you will be the one giving the presentation during the code 
review! 
Don't make it too long, stick to key points, just like you were
presenting a battle plan to a CTO in a small company. Finally, be prepared to have
the presentation interrupted with questions, feel free to keep it in a conversational
format, as this is more of a discussion rather than an academic presentation.

## Final notes
This is a task taken directly from our pipeline just few weeks ago (albeit
simplified, but the core of the issue remains). We cannot wait to hear your 
thoughts on how to solve it! 

Please don't spend more than 2-3 hours preparing, it will not net you extra points, 
and we do not want you to spend enormous amounts of time on a throw-away test. 
Our primary focus is to verify that you do know how to code, and think like a product
owner, who will ultimately have to deliver and be responsible for the delivery.

### Submission
Once you are ready, submit a pull-request with your solution and shoot us an email at 
`jobs@smartia.tech` with a link to your pull-request and copy of your up-to-date CV. 
We will then schedule a call with You as soon as we receive it to present your 
solution (this call usually lasts about 1-2 hours, as we want to get to know one 
another well). **If you have submitted a PR with a solution to the task, you will be 
invited for the call** as we believe that it’s only fair to put in the same amount of 
time as do the candidates.

We hope that you will have fun with it! 

### Using the new ingest command

You can run the new command as follows:

```console
>> cd maio
>> python manage.py ingest ../example.json
```
