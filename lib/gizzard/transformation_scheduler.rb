module Gizzard
  class Transformation::Scheduler

    attr_reader :nameserver, :transformations
    attr_reader :max_copies, :copies_per_host

    def initialize(nameserver, transformations, base_name, max_copies, copies_per_host, poll_interval)
      @nameserver      = nameserver
      @max_copies      = max_copies
      @copies_per_host = copies_per_host
      @transformations = transformations
      @poll_interval   = poll_interval

      @jobs_in_progress = []
      @jobs_finished    = []

      @jobs_pending = transformations.map do |transformation, forwardings_to_shards|
        transformation.bind(base_name, forwardings_to_shards)
      end.flatten
    end

    # to schedule a job:
    # 1. pull a job that does not involve a disqualified host.
    # 2. run prepare ops
    # 3. reload app servers
    # 4. schedule copy
    # 5. put in jobs_in_progress

    # on job completion:
    # 1. run cleanup ops
    # 2. remove from jobs_in_progress
    # 3. put in jos_finished
    # 4. schedule a new job or reload app servers.

    def apply!
      loop do
        @busy_shards = nameserver.get_busy_shards
        cleanup_jobs
        schedule_jobs(max_copies - @busy_shards.length)

        break if @jobs_pending.empty? && @jobs_in_progress.empty?
        sleep @poll_interval
      end

      nameserver.reload_config
    end

    def schedule_jobs(num_to_schedule)
      jobs = (1..num_to_schedule).map do
        job = @jobs_pending.find do |j|
          (busy_hosts & job.involved_hosts).empty?
        end

        @jobs_pending.delete(job)

        job
      end.compact

      unless jobs.empty?
        jobs.each {|j| j.prepare!(nameserver) }

        nameserver.reload_config

        jobs.each {|j| j.copy!(nameserver) }

        @jobs_in_progress.concat(jobs)
      end
    end

    def cleanup_jobs
      jobs = jobs_completed
      @jobs_in_progress -= jobs

      jobs.each {|j| j.cleanup!(nameserver) }

      @jobs_finished.concat(jobs)
    end

    def jobs_completed
      @jobs_in_progress.select {|j| (@busy_shards & j.involved_shards).empty? }
    end

    def busy_hosts
      @busy_shards.map {|s| s.hostname }
    end
  end
end
