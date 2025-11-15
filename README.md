# Pipestream AI - Organization Homepage

This repository serves as the official homepage for the Pipestream AI organization, deployed via GitHub Pages.

## 🌐 Live Site

Once configured, the site will be accessible at:
- **Primary Domain**: https://pipestream.ai
- **Alternative**: https://docs.pipestream.ai (if you prefer a subdomain)
- **GitHub Pages URL**: https://ai-pipestream.github.io/.github/

## 🚀 Quick Start

1. **Enable GitHub Pages** (one-time setup):
   - Go to [Repository Settings → Pages](https://github.com/ai-pipestream/.github/settings/pages)
   - Under "Source", select **GitHub Actions**
   - The workflow will automatically deploy on push to `main`

2. **Configure Custom Domain**:
   - See [SETUP.md](SETUP.md) for detailed DNS configuration instructions
   - GitHub will automatically provide free SSL certificates via Let's Encrypt

3. **Make Updates**:
   - Edit `index.html` to update the homepage
   - Push to `main` branch - changes deploy automatically within 1-2 minutes

## 📁 Repository Structure

```
.
├── index.html              # Main homepage
├── CNAME                   # Custom domain configuration
├── SETUP.md               # Detailed setup instructions
├── README.md              # This file
└── .github/
    └── workflows/
        └── pages.yml      # GitHub Actions deployment workflow
```

## 🔒 SSL/HTTPS

GitHub Pages provides **automatic HTTPS** with Let's Encrypt certificates:
- ✅ Free and automatic
- ✅ Auto-renewing
- ✅ Supports custom domains
- ✅ No manual certificate management needed

If you have a custom wildcard SSL certificate you want to use, see the "Custom SSL Certificate" section in [SETUP.md](SETUP.md) for alternative approaches using Cloudflare or other CDN providers.

## 📝 Customization

The homepage is built with vanilla HTML/CSS for simplicity and fast loading. To customize:

1. Open `index.html`
2. Modify content, styling, or structure
3. Commit and push to `main`
4. GitHub Actions will automatically deploy your changes

## 🛠️ Development

Preview changes locally:
```bash
# Simple Python server
python3 -m http.server 8000

# Or with Node.js
npx http-server
```

Then visit http://localhost:8000

## 📚 Documentation

For complete setup instructions, including DNS configuration and SSL setup, see [SETUP.md](SETUP.md).

## 🤝 Contributing

This is the organization's public face. For changes to the homepage, please open a pull request with your proposed updates.

## 📄 License

Content © 2025 Pipestream AI. All rights reserved.
