/**
 * DataKilnWorks Documentation Client JavaScript
 * Minimalist, high-performance interactions matching Databricks documentation.
 */

(function () {
  'use strict';

  // 1. Theme Management (Light / Dark)
  const THEME_STORAGE_KEY = 'datakilnworks_docs_theme';
  const themeToggleBtn = document.getElementById('theme-toggle-btn');
  const themeIcon = document.getElementById('theme-icon');

  function getPreferredTheme() {
    const saved = localStorage.getItem(THEME_STORAGE_KEY);
    if (saved) return saved;
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches
      ? 'dark'
      : 'light';
  }

  function applyTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem(THEME_STORAGE_KEY, theme);
    if (themeIcon) {
      themeIcon.className = theme === 'dark' ? 'ph ph-sun' : 'ph ph-moon';
    }
  }

  // Initial Theme Application
  applyTheme(getPreferredTheme());

  if (themeToggleBtn) {
    themeToggleBtn.addEventListener('click', function () {
      const current = document.documentElement.getAttribute('data-theme') || 'light';
      const next = current === 'dark' ? 'light' : 'dark';
      applyTheme(next);
    });
  }

  // 2. Code Copy to Clipboard
  document.querySelectorAll('.copy-btn').forEach(function (button) {
    button.addEventListener('click', function () {
      const targetContainer = button.closest('.code-container');
      const codeElement = targetContainer ? targetContainer.querySelector('pre code') : null;
      if (!codeElement) return;

      const codeText = codeElement.innerText;
      navigator.clipboard.writeText(codeText).then(function () {
        const originalHtml = button.innerHTML;
        button.innerHTML = '<i class="ph ph-check"></i> Copied!';
        button.style.backgroundColor = 'rgba(74, 222, 128, 0.2)';
        button.style.color = '#4ade80';

        setTimeout(function () {
          button.innerHTML = originalHtml;
          button.style.backgroundColor = '';
          button.style.color = '';
        }, 2000);
      }).catch(function (err) {
        console.error('Failed to copy code: ', err);
      });
    });
  });

  // 3. Tabbed Code Containers
  document.querySelectorAll('.tabbed-container').forEach(function (container) {
    const buttons = container.querySelectorAll('.tab-btn');
    const contents = container.querySelectorAll('.tab-content');

    buttons.forEach(function (btn) {
      btn.addEventListener('click', function () {
        const targetId = btn.getAttribute('data-tab');

        buttons.forEach(b => b.classList.remove('active'));
        contents.forEach(c => c.classList.remove('active'));

        btn.classList.add('active');
        const activeContent = container.querySelector(`.tab-content[data-tab-content="${targetId}"]`);
        if (activeContent) {
          activeContent.classList.add('active');
        }
      });
    });
  });

  // 4. Client-side Search Filter
  const searchInput = document.getElementById('docs-search');
  if (searchInput) {
    searchInput.addEventListener('input', function () {
      const query = this.value.toLowerCase().trim();
      const contentSections = document.querySelectorAll('.doc-section');
      const sidebarItems = document.querySelectorAll('.sidebar-nav-item');

      if (!query) {
        contentSections.forEach(s => s.style.display = '');
        sidebarItems.forEach(i => i.style.display = '');
        return;
      }

      // Filter sidebar items
      sidebarItems.forEach(function (item) {
        const text = item.textContent.toLowerCase();
        if (text.includes(query)) {
          item.style.display = 'flex';
        } else {
          item.style.display = 'none';
        }
      });

      // Filter content sections
      contentSections.forEach(function (section) {
        const text = section.textContent.toLowerCase();
        if (text.includes(query)) {
          section.style.display = '';
        } else {
          section.style.display = 'none';
        }
      });
    });

    // Keyboard shortcut (Ctrl+K or /)
    window.addEventListener('keydown', function (e) {
      if ((e.ctrlKey && e.key.toLowerCase() === 'k') || (e.key === '/' && document.activeElement !== searchInput)) {
        e.preventDefault();
        searchInput.focus();
        searchInput.select();
      }
    });
  }

  // 5. ScrollSpy & In-Page Table of Contents Highlighting
  const sections = document.querySelectorAll('h2[id], h3[id]');
  const tocLinks = document.querySelectorAll('.toc-link');
  const sidebarLinks = document.querySelectorAll('.sidebar-nav-item');

  function updateActiveNav() {
    let currentId = '';
    const scrollPos = window.scrollY + 100;

    sections.forEach(function (section) {
      const top = section.offsetTop;
      if (scrollPos >= top) {
        currentId = section.getAttribute('id');
      }
    });

    if (currentId) {
      tocLinks.forEach(function (link) {
        if (link.getAttribute('href') === `#${currentId}`) {
          link.classList.add('active');
        } else {
          link.classList.remove('active');
        }
      });

      sidebarLinks.forEach(function (link) {
        if (link.getAttribute('href') === `#${currentId}`) {
          link.classList.add('active');
        } else {
          link.classList.remove('active');
        }
      });
    }
  }

  window.addEventListener('scroll', updateActiveNav, { passive: true });
  updateActiveNav();

  // 6. Mobile Sidebar Drawer
  const mobileToggle = document.getElementById('mobile-menu-toggle');
  const sidebar = document.querySelector('.docs-sidebar');

  if (mobileToggle && sidebar) {
    mobileToggle.addEventListener('click', function () {
      sidebar.classList.toggle('open');
    });

    // Close on navigation
    document.querySelectorAll('.sidebar-nav-item').forEach(function (link) {
      link.addEventListener('click', function () {
        sidebar.classList.remove('open');
      });
    });
  }

  // 7. Syntax Highlighting (Highlight.js)
  function initSyntaxHighlighting() {
    if (typeof window.hljs === 'undefined') {
      console.warn('highlight.js not loaded');
      return;
    }

    window.hljs.configure({ ignoreUnescapedHTML: true });

    document.querySelectorAll('pre code').forEach(function (block) {
      if (block.classList.contains('hljs')) return;

      const rawText = block.textContent;
      const trimmed = rawText.trim();

      // Skip ASCII diagrams (like architecture boxes starting with ┌ or +--)
      if (trimmed.startsWith('┌') || trimmed.startsWith('+--') || trimmed.startsWith('|')) {
        return;
      }

      // 1. Detect language from class (e.g. language-sql, language-yaml, language-yml, language-python)
      const classes = Array.from(block.classList);
      let lang = null;
      for (const cls of classes) {
        if (cls.startsWith('language-')) {
          lang = cls.replace('language-', '').toLowerCase();
          break;
        } else if (cls.startsWith('lang-')) {
          lang = cls.replace('lang-', '').toLowerCase();
          break;
        }
      }

      // 2. Fallback: check code-lang-tag in header
      if (!lang) {
        const container = block.closest('.code-container');
        const langTag = container ? container.querySelector('.code-lang-tag') : null;
        if (langTag) {
          const tagText = langTag.textContent.toLowerCase().trim();
          if (tagText.includes('sql')) lang = 'sql';
          else if (tagText.includes('yaml') || tagText.includes('yml')) lang = 'yaml';
          else if (tagText.includes('python') || tagText.includes('py')) lang = 'python';
          else if (tagText.includes('bash') || tagText.includes('sh')) lang = 'bash';
          else if (tagText.includes('json')) lang = 'json';
          else if (tagText.includes('html')) lang = 'html';
        }
      }

      // 3. Normalize language aliases
      const aliasMap = {
        'yml': 'yaml',
        'yaml': 'yaml',
        'sql': 'sql',
        'py': 'python',
        'python': 'python',
        'sh': 'bash',
        'bash': 'bash',
        'shell': 'bash',
        'json': 'json',
        'html': 'html',
        'xml': 'xml',
        'js': 'javascript',
        'javascript': 'javascript'
      };
      if (lang && aliasMap[lang]) {
        lang = aliasMap[lang];
      }

      try {
        let highlighted = '';
        if (lang && window.hljs.getLanguage(lang)) {
          highlighted = window.hljs.highlight(rawText, { language: lang, ignoreIllegals: true }).value;
        } else {
          const autoRes = window.hljs.highlightAuto(rawText);
          highlighted = autoRes.value;
        }

        // Jinja syntax enhancement for dbt models in SQL
        if (lang === 'sql' && (rawText.includes('{{') || rawText.includes('{%'))) {
          highlighted = highlighted.replace(new RegExp('\\{\\{|\\}\\}', 'g'), m => '<span class="hljs-jinja-delim">' + m + '</span>');
          highlighted = highlighted.replace(new RegExp('\\{%|%\\}', 'g'), m => '<span class="hljs-jinja-delim">' + m + '</span>');
          highlighted = highlighted.replace(/\b(ref|source|config|var|env_var|is_incremental|adapter|doc|return)\b(?=\s*\()/g, m => '<span class="hljs-jinja-macro">' + m + '</span>');
        }

        block.innerHTML = highlighted;
        block.classList.add('hljs');
      } catch (e) {
        console.warn('Syntax highlight error:', e);
      }
    });
  }

  // Initialize highlighting immediately or on DOMContentLoaded
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initSyntaxHighlighting);
  } else {
    initSyntaxHighlighting();
  }
})();
