document.addEventListener('DOMContentLoaded', () => {

  // 1. Mobile Menu Drawer Toggle & Resize
  const mobileToggle = document.getElementById('mobile-toggle');
  const navLinks = document.getElementById('nav-links');

  if (mobileToggle && navLinks) {
    const toggleMenu = (open) => {
      const isOpen = open !== undefined ? open : !navLinks.classList.contains('mobile-open');
      if (isOpen) {
        navLinks.classList.add('mobile-open');
        mobileToggle.innerHTML = `<i class="fa-solid fa-xmark"></i>`;
        document.body.style.overflow = 'hidden';
      } else {
        navLinks.classList.remove('mobile-open');
        mobileToggle.innerHTML = `<i class="fa-solid fa-bars"></i>`;
        document.body.style.overflow = '';
      }
    };

    mobileToggle.addEventListener('click', () => toggleMenu());

    // Reset menu on resize to desktop mode
    window.addEventListener('resize', () => {
      if (window.innerWidth > 868 && navLinks.classList.contains('mobile-open')) {
        toggleMenu(false);
      }
    });
  }

  // 2. Smooth Navigation Scroll & Impressive Section Transition Effect
  const navItems = document.querySelectorAll('.nav-link');
  const sections = document.querySelectorAll('.section, .hero');

  navItems.forEach(link => {
    link.addEventListener('click', (e) => {
      const href = link.getAttribute('href');
      if (href.startsWith('#')) {
        e.preventDefault();
        const targetSection = document.querySelector(href);

        if (targetSection) {
          // Close mobile menu if open
          if (navLinks && navLinks.classList.contains('mobile-open')) {
            navLinks.classList.remove('mobile-open');
            if (mobileToggle) mobileToggle.innerHTML = `<i class="fa-solid fa-bars"></i>`;
            document.body.style.overflow = '';
          }

          // Smooth Eased Scroll to Target Section
          const headerOffset = 60;
          const elementPosition = targetSection.getBoundingClientRect().top;
          const offsetPosition = elementPosition + window.pageYOffset - headerOffset;

          window.scrollTo({
            top: offsetPosition,
            behavior: 'smooth'
          });

          // Trigger Impressive Spotlight Glow Flash Animation on Target Section
          sections.forEach(s => s.classList.remove('section-focused'));
          
          setTimeout(() => {
            targetSection.classList.add('section-focused');
          }, 300);

          setTimeout(() => {
            targetSection.classList.remove('section-focused');
          }, 1900);
        }
      }
    });
  });

  // 3. Scroll Active Navbar Link Tracking
  const onScroll = () => {
    let current = '';
    const scrollPosition = window.pageYOffset + 120;

    sections.forEach(section => {
      const sectionTop = section.offsetTop;
      const sectionHeight = section.offsetHeight;
      if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
        current = section.getAttribute('id');
      }
    });

    navItems.forEach(item => {
      item.classList.remove('active');
      if (item.getAttribute('href') === `#${current}`) {
        item.classList.add('active');
      }
    });
  };

  window.addEventListener('scroll', onScroll);
  onScroll();

  // 4. Element Entrance Scroll Reveal Observer
  const revealElements = document.querySelectorAll('.card, .section-header, .hero-terminal, .metric-card');
  
  revealElements.forEach(el => {
    el.classList.add('reveal-element');
  });

  const revealObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.classList.add('revealed');
      }
    });
  }, {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
  });

  revealElements.forEach(el => revealObserver.observe(el));

  // 5. Interactive Architecture Stage Filter & Details Viewer
  const archTabs = document.querySelectorAll('.arch-tab');
  const stageGroups = document.querySelectorAll('.stage-group');
  const archDetailsTitle = document.getElementById('arch-details-title');
  const archDetailsText = document.getElementById('arch-details-text');

  const stageDescriptions = {
    all: {
      title: "Full Enterprise Claims ETL Architecture",
      text: "End-to-end cloud modernization workflow: FMH On-Prem SFTP file ingestion into S3, PySpark Glue dynamic JSON staging, AWS Step Functions 150-record batch REST API caller, Db2 Stored Procedure persistence, and AWS Bedrock AI failure diagnostics."
    },
    s1: {
      title: "Stage 1: File Ingestion & Storage",
      text: "On-premise FMH team deposits multi-vendor insurance files (CSV, XLSX, ZIP, DAT) via AWS Transfer Family (SFTP) into s3://.../vendor_a/inbound/. Glue Job 1 moves files to source/ to ensure single execution."
    },
    s2: {
      title: "Stage 2: Dynamic Staging & Schema Mapping",
      text: "PySpark reads vendor-specific JSON config from S3 (specifying target data types, date formats like yyyy-MM-dd, delimiters). Dynamic PySpark engine validates schema and writes to Db2 / Aurora STG_DISABILITY_CLAIMS table."
    },
    s3: {
      title: "Stage 3: Step Functions Orchestration & 150-Record Batch API",
      text: "AWS Step Functions triggers Glue Job 2 to partition staged claims into chunks of 150 records per S3 JSON batch request. AWS Lambda retrieves Secrets Manager tokens, POSTs to external Claims REST API, and persists response JSONs."
    },
    s4: {
      title: "Stage 4: Database Persistence & Db2 Stored Procedure Load",
      text: "Glue Job 3 parses partial-success API responses. Invokes IBM Db2 Stored Procedure (SP_PROCESS_CLAIM_CREATION), inserts valid records into DISABILITY_CLAIMS aurora table, and moves processed files to history for idempotent reruns."
    },
    s5: {
      title: "Stage 5: AI Observability (AWS Bedrock) & Flyway CI/CD",
      text: "CloudWatch failure alarms invoke Lambda + AWS Bedrock (Claude 3) to summarize complex stack traces into plain-English root-cause alerts sent via SNS to Slack/Email. GitHub Actions deploys Flyway DDL changes to Dev/QA databases."
    }
  };

  archTabs.forEach(tab => {
    tab.addEventListener('click', () => {
      const selectedStage = tab.getAttribute('data-stage');

      archTabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');

      stageGroups.forEach(group => {
        if (selectedStage === 'all' || group.classList.contains(`stage-${selectedStage}`)) {
          group.style.opacity = '1';
          group.classList.add('active');
        } else {
          group.style.opacity = '0.25';
          group.classList.remove('active');
        }
      });

      if (stageDescriptions[selectedStage]) {
        archDetailsTitle.innerHTML = `<i class="fa-solid fa-circle-info"></i> ${stageDescriptions[selectedStage].title}`;
        archDetailsText.textContent = stageDescriptions[selectedStage].text;
      }
    });
  });

  // 6. Live Bedrock Log Diagnostic Demo Simulator
  const runDemoBtn = document.getElementById('run-demo-btn');
  const consoleOutput = document.getElementById('console-output');

  if (runDemoBtn && consoleOutput) {
    runDemoBtn.addEventListener('click', () => {
      runDemoBtn.disabled = true;
      runDemoBtn.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> Analyzing...`;

      consoleOutput.innerHTML = `
        <div class="console-line text-muted"><i class="fa-solid fa-arrow-right"></i> Extracting last 100 lines of CloudWatch LogStream...</div>
        <div class="console-line text-muted"><i class="fa-solid fa-arrow-right"></i> Invoking AWS Bedrock API (Model: anthropic.claude-3-sonnet)...</div>
      `;

      setTimeout(() => {
        consoleOutput.innerHTML = `
          <div style="color: #30d158; font-weight: bold; margin-bottom: 6px;">
            🚨 [AWS Bedrock SNS Alert Output]:
          </div>
          <div style="color: #f5f5f7; font-size: 0.82rem; line-height: 1.5; padding-left: 8px; border-left: 2px solid #30d158; word-break: break-word;">
            <strong>Job Name:</strong> claim_stage_load<br>
            <strong>Root Cause:</strong> Schema Mismatch — Column <code>'incurral_date'</code> expected format <code>YYYY-MM-DD</code>, but received <code>'09/24/2026'</code> at Line 4,120.<br>
            <strong>Affected File:</strong> <code>s3://prudential-data-lake/vendor_b/inbound/claims.csv</code><br>
            <strong>Action Required:</strong> Update <code>vendor_b.json</code> date_format property to <code>MM/dd/yyyy</code> or request vendor re-feed.
          </div>
        `;
        runDemoBtn.disabled = false;
        runDemoBtn.innerHTML = `Simulate AI Diagnostic`;
      }, 1600);
    });
  }

  // 7. Metric Counter Animation
  const metricValues = document.querySelectorAll('.metric-value');
  let animated = false;

  const animateMetrics = () => {
    const metricsSection = document.getElementById('metrics');
    if (!metricsSection) return;

    const sectionPos = metricsSection.getBoundingClientRect().top;
    const screenPos = window.innerHeight / 1.15;

    if (sectionPos < screenPos && !animated) {
      metricValues.forEach(valueElement => {
        const targetText = valueElement.innerText;
        const numericValue = parseInt(targetText.replace(/\D/g, ''));
        const suffix = targetText.replace(/[0-9]/g, '');

        if (!isNaN(numericValue)) {
          let count = 0;
          const speed = Math.ceil(numericValue / 25);
          const updateCount = setInterval(() => {
            count += speed;
            if (count >= numericValue) {
              valueElement.innerText = numericValue + suffix;
              clearInterval(updateCount);
            } else {
              valueElement.innerText = count + suffix;
            }
          }, 40);
        }
      });
      animated = true;
    }
  };

  window.addEventListener('scroll', animateMetrics);
  animateMetrics();
});
