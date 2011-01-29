require "set"

module Gizzard
  def self.schedule!(*args)
    Transformation::Scheduler.new(*args).apply!
  end

  class Transformation::Scheduler

    attr_reader :nameserver, :transformations
    attr_reader :max_copies, :copies_per_host

    DEFAULT_OPTIONS = {
      :max_copies      => 30,
      :copies_per_host => 8,
      :poll_interval   => 10
    }.freeze

    def initialize(nameserver, base_name, transformations, options = {})
      options = DEFAULT_OPTIONS.merge(options)
      @nameserver         = nameserver
      @transformations    = transformations
      @max_copies         = options[:max_copies]
      @copies_per_host    = options[:copies_per_host]
      @poll_interval      = options[:poll_interval]
      @be_quiet           = options[:quiet]
      @dont_show_progress = options[:no_progress] || @be_quiet

      @jobs_in_progress = []
      @jobs_finished    = []

      @jobs_pending = Set.new(transformations.map do |transformation, forwardings_to_shards|
        transformation.bind(base_name, forwardings_to_shards)
      end.flatten)
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
      @start_time = Time.now

      loop do
        reload_busy_shards

        cleanup_jobs
        schedule_jobs(max_copies - busy_shards.length)

        break if @jobs_pending.empty? && @jobs_in_progress.empty?

        unless nameserver.dryrun?
          if @dont_show_progress
            sleep(@poll_interval)
          else
            sleep_with_progress(@poll_interval)
          end
        end
      end

      nameserver.reload_updated_forwardings

      log "#{@jobs_finished.length} transformation#{'s' if @jobs_finished.length > 1} applied. Total time elapsed: #{time_elapsed}"
    end

    def schedule_jobs(num_to_schedule)
      to_be_busy_hosts = []
      jobs             = []

      @jobs_pending.each do |j|
        if (busy_hosts(to_be_busy_hosts) & j.involved_hosts).empty?
          jobs << j
          to_be_busy_hosts.concat j.involved_hosts_array

          break if jobs.length == num_to_schedule
        end
      end

      @jobs_pending.subtract(jobs)

      jobs = jobs.sort_by {|t| t.forwarding }

      unless jobs.empty?
        log "STARTING:"
        jobs.each do |j|
          log "  #{j.inspect}"
          j.prepare!(nameserver)
        end

        nameserver.reload_updated_forwardings

        copy_jobs = jobs.select {|j| j.copy_required? }

        unless copy_jobs.empty?
          log "COPIES:"
          copy_jobs.each do |j|
            j.copy_descs.each {|d| log "  #{d}" }
            j.copy!(nameserver)
          end

          reload_busy_shards
        end

        @jobs_in_progress.concat(jobs)
      end
    end

    def cleanup_jobs
      jobs = jobs_completed

      unless jobs.empty?
        @jobs_in_progress -= jobs

        log "FINISHING:"
        jobs.each do |j|
          log "  #{j.inspect}"
          j.cleanup!(nameserver)
        end

        @jobs_finished.concat(jobs)
      end
    end

    def jobs_completed
      @jobs_in_progress.select {|j| (busy_shards & j.involved_shards).empty? }
    end

    def reload_busy_shards
      @busy_shards = nil
      busy_shards
    end

    def busy_shards
      @busy_shards ||=
        if nameserver.dryrun?
          Set.new
        else
          nameserver.get_busy_shards.inject(Set.new) {|set, shard| set.add(shard.id) }
        end
    end

    def busy_hosts(extra_hosts = [])
      hosts = extra_hosts + busy_shards.map {|s| s.hostname }

      copies_count_map = hosts.inject({}) do |h, host|
        h.update(host => 1) {|_,a,b| a + b }
      end

      copies_count_map.select {|_, count| count >= @copies_per_host }.inject(Set.new) {|set,(host, _)| set.add(host) }
    end

    def sleep_with_progress(interval)
      start = Time.now
      while (Time.now - start) < interval
        put_copy_progress
        sleep 0.2
      end
    end

    def clear_progress_string
      if @progress_string
        print "\r" + (" " * (@progress_string.length + 10)) + "\r"
        @progress_string = nil
      end
    end

    def log(*args)
      unless @be_quiet
        clear_progress_string
        puts *args
      end
    end

    def put_copy_progress
      @i ||= 0
      @i  += 1

      unless @jobs_in_progress.empty? || busy_shards.empty?
        spinner         = ['-', '\\', '|', '/'][@i % 4]
        elapsed_txt     = "Time elapsed: #{time_elapsed}"
        pending_txt     = "Pending: #{@jobs_pending.length}"
        finished_txt    = "Finished: #{@jobs_finished.length}"
        in_progress_txt =
          if busy_shards.length != @jobs_in_progress.length
            "In progress: #{@jobs_in_progress.length} (Copies: #{busy_shards.length})"
          else
            "In progress: #{@jobs_in_progress.length}"
          end

        clear_progress_string

        @progress_string = "#{spinner} #{in_progress_txt} #{pending_txt} #{finished_txt} #{elapsed_txt}"
        print @progress_string; $stdout.flush
      end
    end

    def time_elapsed
      s = (Time.now - @start_time).to_i

      if s == 1
        "1 second"
      elsif s < 60
        "#{s} seconds"
      else
        days    = s / (60 * 60 * 24)               if s >= 60 * 60 * 24
        hours   = (s % (60 * 60 * 24)) / (60 * 60) if s >= 60 * 60
        minutes = (s % (60 * 60)) / 60             if s >= 60
        seconds = s % 60

        [days,hours,minutes,seconds].compact.map {|i| "%0.2i" % i }.join(":")
      end
    end
  end
end
